package rabbitmq

import (
	"context"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const recoveryExchange = "recovery-test-x"

// fatalRecordingLogger records whether the library ever escalated to Fatalf,
// which in production exits the process and takes every other channel in the
// pod down with it (INC-357).
type fatalRecordingLogger struct {
	t     *testing.T
	fatal atomic.Bool
	done  atomic.Bool
}

func (l *fatalRecordingLogger) logf(format string, v ...interface{}) {
	if l.done.Load() {
		return
	}
	l.t.Logf(format, v...)
}

func (l *fatalRecordingLogger) Fatalf(format string, v ...interface{}) {
	l.fatal.Store(true)
	if !l.done.Load() {
		l.t.Errorf("unexpected Fatalf: "+format, v...)
	}
}

func (l *fatalRecordingLogger) Errorf(format string, v ...interface{}) { l.logf(format, v...) }
func (l *fatalRecordingLogger) Warnf(format string, v ...interface{})  { l.logf(format, v...) }
func (l *fatalRecordingLogger) Infof(format string, v ...interface{})  { l.logf(format, v...) }
func (l *fatalRecordingLogger) Debugf(format string, v ...interface{}) { l.logf(format, v...) }

// redeclareExchange replaces recoveryExchange out from under the publisher over
// a raw connection. Declaring it non-durable makes the publisher's next durable
// redeclare fail with 406 PRECONDITION_FAILED, which closes its channel.
func redeclareExchange(t *testing.T, connStr string, durable bool) {
	t.Helper()
	rawConn, err := amqp.Dial(connStr)
	if err != nil {
		t.Fatalf("raw dial failed: %v", err)
	}
	defer rawConn.Close()
	rawCh, err := rawConn.Channel()
	if err != nil {
		t.Fatalf("raw channel failed: %v", err)
	}
	if err := rawCh.ExchangeDelete(recoveryExchange, false, false); err != nil {
		t.Fatalf("raw exchange delete failed: %v", err)
	}
	if err := rawCh.ExchangeDeclare(recoveryExchange, "direct", durable, false, false, false, nil); err != nil {
		t.Fatalf("raw exchange declare failed: %v", err)
	}
}

// TestPublisherKeepsRecoveringAfterFailedRedeclare is the INC-357 regression
// test: a redeclare that fails after a reconnect must log and wait for the next
// reconnect, not call Fatalf and kill the process, and the publisher must still
// recover once the broker-side conflict is resolved.
func TestPublisherKeepsRecoveringAfterFailedRedeclare(t *testing.T) {
	connStr, containerID := prepareDockerTestWithContainerID(t)
	waitForBrokerReady(t, containerID)

	logger := &fatalRecordingLogger{t: t}
	t.Cleanup(func() { logger.done.Store(true) })

	conn, err := NewConn(connStr,
		WithConnectionOptionsReconnectInterval(200*time.Millisecond),
		WithConnectionOptionsLogger(logger),
	)
	if err != nil {
		t.Fatalf("error creating connection: %v", err)
	}
	defer conn.Close()

	publisher, err := NewPublisher(conn,
		WithPublisherOptionsExchangeName(recoveryExchange),
		WithPublisherOptionsExchangeDeclare,
		WithPublisherOptionsExchangeKind("direct"),
		WithPublisherOptionsExchangeDurable,
		WithPublisherOptionsConfirm,
		WithPublisherOptionsLogger(logger),
	)
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}

	defer publisher.Close()
	returns := make(chan Return, 1)
	publisher.NotifyReturn(func(r Return) {
		select {
		case returns <- r:
		default:
		}
	})

	redeclareExchange(t, connStr, false)

	if out, err := exec.Command("docker", "exec", "--user", "rabbitmq", containerID,
		"rabbitmqctl", "close_all_connections", "test").CombinedOutput(); err != nil {
		t.Fatalf("failed to close connections: %v\n%s", err, out)
	}

	// The reconnect restores the channel (1), the failed durable redeclare makes
	// the broker close it, and the manager reconnects again (2).
	deadline := time.Now().Add(15 * time.Second)
	for publisher.chanManager.GetReconnectionCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: reconnection count is %d, want >= 2", publisher.chanManager.GetReconnectionCount())
		}
		if logger.fatal.Load() {
			t.Fatal("publisher escalated to Fatalf instead of waiting for the next reconnect")
		}
		time.Sleep(100 * time.Millisecond)
	}

	redeclareExchange(t, connStr, true)

	rawConn, err := amqp.Dial(connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer rawConn.Close()
	rawCh, err := rawConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := rawCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawCh.QueueBind(queue.Name, "k", recoveryExchange, false, nil); err != nil {
		t.Fatal(err)
	}
	deliveries, err := rawCh.Consume(queue.Name, "", true, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	deadline = time.Now().Add(15 * time.Second)
	for {
		confirms, err := publisher.PublishWithDeferredConfirmWithContext(ctx, []byte("x"), []string{"k"}, WithPublishOptionsExchange(recoveryExchange))
		if err == nil && len(confirms) == 1 && confirms[0] != nil {
			ack, err := confirms[0].WaitContext(ctx)
			if err == nil && ack {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher never recovered: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	select {
	case d := <-deliveries:
		if string(d.Body) != "x" {
			t.Fatalf("unexpected delivery: %q", d.Body)
		}
	case <-ctx.Done():
		t.Fatal("recovered publisher did not route a message")
	}
	if err := publisher.PublishWithContext(ctx, []byte("unroutable"), []string{"missing"}, WithPublishOptionsExchange(recoveryExchange), WithPublishOptionsMandatory); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-returns:
		if r.ReplyCode != 312 || string(r.Body) != "unroutable" {
			t.Fatalf("unexpected return: %+v", r)
		}
	case <-ctx.Done():
		t.Fatal("return handler did not recover")
	}
	if logger.fatal.Load() {
		t.Fatal("publisher escalated to Fatalf")
	}

	closed := make(chan struct{})
	go func() {
		publisher.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher.Close() did not return within 5s")
	}
}
