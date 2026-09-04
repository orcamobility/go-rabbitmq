package rabbitmq

import (
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
		WithPublisherOptionsLogger(logger),
	)
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}

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

	deadline = time.Now().Add(15 * time.Second)
	for {
		err := publisher.Publish([]byte("x"), []string{"k"}, WithPublishOptionsExchange(recoveryExchange))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher never recovered: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	if logger.fatal.Load() {
		t.Fatal("publisher escalated to Fatalf instead of waiting for the next reconnect")
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
