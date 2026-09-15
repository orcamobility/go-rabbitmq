package rabbitmq

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type confirmFrameConn struct {
	net.Conn
	pending     []byte
	readMethod  func(uint16, uint16) bool
	writeMethod func(uint16, uint16)
}

func (c *confirmFrameConn) Read(p []byte) (int, error) {
	for len(c.pending) == 0 {
		header := make([]byte, 7)
		if _, err := io.ReadFull(c.Conn, header); err != nil {
			return 0, err
		}
		payload := make([]byte, int(binary.BigEndian.Uint32(header[3:]))+1)
		if _, err := io.ReadFull(c.Conn, payload); err != nil {
			return 0, err
		}
		if header[0] == 1 && len(payload) >= 5 && c.readMethod != nil && c.readMethod(binary.BigEndian.Uint16(payload), binary.BigEndian.Uint16(payload[2:])) {
			continue
		}
		c.pending = append(header, payload...)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *confirmFrameConn) Write(p []byte) (int, error) {
	for frame := p; len(frame) >= 8 && frame[0] != 'A'; {
		size := int(binary.BigEndian.Uint32(frame[3:])) + 8
		if size > len(frame) {
			break
		}
		if frame[0] == 1 && size >= 12 && c.writeMethod != nil {
			c.writeMethod(binary.BigEndian.Uint16(frame[7:]), binary.BigEndian.Uint16(frame[9:]))
		}
		frame = frame[size:]
	}
	return c.Conn.Write(p)
}

func confirmedRoutingAddress(t *testing.T) string {
	t.Helper()
	if address := os.Getenv("TEST_AMQP_URL"); address != "" {
		return address
	}
	address, id := prepareDockerTestWithContainerID(t)
	waitForBrokerReady(t, id)
	return address
}

func newConfirmFrameConnection(t *testing.T, address string, read func(uint16, uint16) bool, write func(uint16, uint16)) *Conn {
	t.Helper()
	conn, err := NewConn(address, WithConnectionOptionsReconnectInterval(10*time.Millisecond), WithConnectionOptionsReconnectMaxInterval(50*time.Millisecond), WithConnectionOptionsConfig(Config{
		Dial: func(network, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(network, addr, 3*time.Second)
			if err != nil {
				return nil, err
			}
			return &confirmFrameConn{Conn: c, readMethod: read, writeMethod: write}, nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestConfirmedRoutingCloseWhileAwaitingAck(t *testing.T) {
	ack := make(chan struct{}, 1)
	conn := newConfirmFrameConnection(t, confirmedRoutingAddress(t), func(class, method uint16) bool {
		if class == 60 && method == 80 {
			ack <- struct{}{}
			return true
		}
		return false
	}, nil)
	p, err := NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	published := make(chan error, 1)
	go func() {
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("close"), []string{"missing"})
		published <- err
	}()
	select {
	case <-ack:
	case <-time.After(3 * time.Second):
		t.Fatal("broker did not acknowledge publish")
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		cancel()
		<-closed
		t.Fatal("Close blocked while waiting for confirmation")
	}
	select {
	case err := <-published:
		if err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish did not exit after Close")
	}
	_, err = p.PublishWithConfirmedRoutingWithContext(ctx, nil, []string{"missing"})
	if !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("publish after Close: %v", err)
	}
}

func TestConfirmedRoutingSerializesRecoverySetup(t *testing.T) {
	var armed atomic.Bool
	var once sync.Once
	confirmReply := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	declared := make(chan struct{}, 10)
	conn := newConfirmFrameConnection(t, confirmedRoutingAddress(t), func(class, method uint16) bool {
		if class == 85 && method == 11 && armed.Load() {
			once.Do(func() { close(confirmReply); <-release })
		}
		return false
	}, func(class, method uint16) {
		if class == 40 && method == 10 && armed.Load() {
			declared <- struct{}{}
		}
	})
	p, err := NewPublisher(conn, WithPublisherOptionsExchangeName("amq.direct"), WithPublisherOptionsExchangeKind("direct"), WithPublisherOptionsExchangeDurable, WithPublisherOptionsExchangeDeclare)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	defer unblock()
	armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	published := make(chan error, 1)
	go func() {
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, nil, []string{"missing"})
		published <- err
	}()
	select {
	case <-confirmReply:
	case <-ctx.Done():
		t.Fatal("confirm setup did not start")
	}
	recovered := make(chan bool, 1)
	go func() { recovered <- p.recoverAfterReconnect(errors.New("test recovery")) }()
	select {
	case <-declared:
		unblock()
		<-published
		<-recovered
		t.Fatal("recovery sent exchange declaration while confirm setup held its reply")
	case <-time.After(200 * time.Millisecond):
	}
	unblock()
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if !<-recovered {
		t.Fatal("recovery failed")
	}
	select {
	case <-declared:
	default:
		t.Fatal("recovery did not declare exchange")
	}
}

func TestConfirmedRoutingCancellationDuringSetup(t *testing.T) {
	address := confirmedRoutingAddress(t)
	for _, deadline := range []bool{false, true} {
		name := "cancelled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			setup := make(chan struct{})
			release := make(chan struct{})
			var once, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var sends atomic.Int32
			conn := newConfirmFrameConnection(t, address, func(class, method uint16) bool {
				if class == 85 && method == 11 {
					once.Do(func() { close(setup); <-release })
				}
				return false
			}, func(class, method uint16) {
				if class == 60 && method == 40 {
					sends.Add(1)
				}
			})
			p, err := NewPublisher(conn)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			defer unblock()
			raw, err := amqp.Dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			ch, err := raw.Channel()
			if err != nil {
				t.Fatal(err)
			}
			queue, err := ch.QueueDeclare("", false, true, true, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			wantErr := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				wantErr = context.DeadlineExceeded
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("cancelled"), []string{queue.Name}, WithPublishOptionsMandatory)
				result <- err
			}()
			select {
			case <-setup:
			case <-time.After(3 * time.Second):
				t.Fatal("confirm setup did not start")
			}
			if deadline {
				<-ctx.Done()
			} else {
				cancel()
			}
			unblock()
			if err := <-result; !errors.Is(err, wantErr) {
				t.Fatalf("want %v, got %v", wantErr, err)
			}
			if sends.Load() != 0 {
				t.Fatal("cancelled request sent a publish frame")
			}
			ctx, finish := context.WithTimeout(context.Background(), 3*time.Second)
			defer finish()
			cs, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("valid"), []string{queue.Name}, WithPublishOptionsMandatory)
			if err != nil || len(cs) != 1 || !cs[0].Acked() {
				t.Fatalf("valid publish after cancelled setup was not confirmed: %v", err)
			}
			msg, ok, err := ch.Get(queue.Name, true)
			if err != nil || !ok || string(msg.Body) != "valid" {
				t.Fatalf("wrong delivery after cancelled setup: %v %t %q", err, ok, msg.Body)
			}
			if _, ok, err := ch.Get(queue.Name, true); err != nil || ok {
				t.Fatalf("cancelled request reached the queue: %v %t", err, ok)
			}
			if sends.Load() != 1 || p.chanManager.GetReconnectionCount() != 0 {
				t.Fatal("expected one publish on the original channel")
			}
		})
	}
}

func TestConfirmedRoutingCancellationRequiresNewChannel(t *testing.T) {
	var dropAck atomic.Bool
	dropAck.Store(true)
	ack := make(chan struct{}, 1)
	conn := newConfirmFrameConnection(t, confirmedRoutingAddress(t), func(class, method uint16) bool {
		if class == 60 && method == 80 && dropAck.Load() {
			ack <- struct{}{}
			return true
		}
		return false
	}, nil)
	p, err := NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	published := make(chan error, 1)
	go func() {
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("old"), []string{"absent"}, WithPublishOptionsMandatory)
		published <- err
	}()
	select {
	case <-ack:
	case <-time.After(3 * time.Second):
		t.Fatal("no acknowledgement to suppress")
	}
	cancel()
	if err := <-published; !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation, got %v", err)
	}
	ctx, finish := context.WithTimeout(context.Background(), 3*time.Second)
	defer finish()
	if _, err := p.PublishWithConfirmedRoutingWithContext(ctx, nil, []string{"absent"}); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("channel with an unclaimed return remained usable: %v", err)
	}
	dropAck.Store(false)
	if _, err := p.chanManager.QueueDeclarePassiveSafe(fmt.Sprintf("pr9-absent-%d", time.Now().UnixNano()), false, false, false, false, nil); err == nil {
		t.Fatal("expected channel exception")
	}
	for {
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("new"), []string{"absent"}, WithPublishOptionsMandatory)
		var returned *ReturnedError
		if errors.As(err, &returned) {
			if string(returned.Body) != "new" {
				t.Fatalf("return assigned to wrong publish: %q", returned.Body)
			}
			return
		}
		if !errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("want closed channel until recovery, got %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("channel did not recover")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestConfirmedRoutingAfterChannelRecovery(t *testing.T) {
	address := confirmedRoutingAddress(t)
	conn := newConfirmFrameConnection(t, address, nil, nil)
	p, err := NewPublisher(conn, WithPublisherOptionsExchangeName("amq.direct"), WithPublisherOptionsExchangeKind("direct"), WithPublisherOptionsExchangeDurable, WithPublisherOptionsExchangeDeclare, WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	returns := make(chan Return, 100)
	confirms := make(chan Confirmation, 100)
	p.NotifyReturn(func(r Return) { returns <- r })
	p.NotifyPublish(func(c Confirmation) { confirms <- c })
	raw, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	ch, err := raw.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(queue.Name, queue.Name, "amq.direct", false, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		before := p.chanManager.GetReconnectionCount()
		if _, err := p.chanManager.QueueDeclarePassiveSafe(fmt.Sprintf("pr9-absent-%d", time.Now().UnixNano()), false, false, false, false, nil); err == nil {
			t.Fatal("expected channel exception")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		body := fmt.Sprintf("recovered-%d", i)
		for {
			cs, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte(body), []string{queue.Name}, WithPublishOptionsExchange("amq.direct"), WithPublishOptionsMandatory)
			if err == nil && len(cs) == 1 && cs[0].Acked() {
				break
			}
			if errors.Is(err, amqp.ErrCommandInvalid) {
				cancel()
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				cancel()
				t.Fatalf("publish did not recover: %v", err)
			case <-time.After(5 * time.Millisecond):
			}
		}
		if p.chanManager.GetReconnectionCount() <= before {
			cancel()
			t.Fatal("channel was not replaced")
		}
		msg, ok, err := ch.Get(queue.Name, true)
		if err != nil || !ok || string(msg.Body) != body {
			cancel()
			t.Fatalf("wrong recovered delivery: %v %t %q", err, ok, msg.Body)
		}
		for {
			_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte(body), []string{"absent-" + queue.Name}, WithPublishOptionsExchange("amq.direct"), WithPublishOptionsMandatory)
			var returned *ReturnedError
			if !errors.As(err, &returned) || returned.ReplyCode != 312 {
				cancel()
				t.Fatalf("want return after recovery, got %v", err)
			}
			select {
			case r := <-returns:
				if string(r.Body) == body {
					goto returned
				}
			case <-time.After(10 * time.Millisecond):
			}
			if ctx.Err() != nil {
				cancel()
				t.Fatal("return handler not restored")
			}
		}
	returned:
		for {
			select {
			case c := <-confirms:
				if c.ReconnectionCount > int(before) {
					cancel()
					goto confirmed
				}
			case <-ctx.Done():
				cancel()
				t.Fatal("confirm handler not restored")
			}
		}
	confirmed:
	}
}
