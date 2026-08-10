package rabbitmq

import (
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Unit tests for the stuck-unacked-delivery fix (ACB-379): a handler that
// exits because its consumer closed must Nack the delivery it is holding.

// recordingAcknowledger records ack/nack calls made through amqp.Delivery.
type recordingAcknowledger struct {
	mu       sync.Mutex
	acks     int
	nacks    int
	requeues []bool
}

func (a *recordingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks++
	return nil
}

func (a *recordingAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nacks++
	a.requeues = append(a.requeues, requeue)
	return nil
}

func (a *recordingAcknowledger) Reject(tag uint64, requeue bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return nil
}

// A delivery already in hand when the consumer closes must be Nack'd with
// requeue so the replacement consumer receives it promptly. Breaking without
// acking leaves it delivered-unacked: invisible, TTL-exempt, and stuck until
// the channel dies.
func TestHandlerGoroutineNacksInFlightDeliveryOnClose(t *testing.T) {
	ack := &recordingAcknowledger{}
	msgs := make(chan amqp.Delivery, 1)
	msgs <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	close(msgs)

	consumer := newClosedTestConsumer()
	handlerGoroutine(consumer, msgs, consumer.options, func(d Delivery) Action {
		t.Error("handler must not run on a closed consumer")
		return Ack
	})

	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.nacks != 1 || len(ack.requeues) != 1 || !ack.requeues[0] {
		t.Fatalf("in-flight delivery on closed consumer: nacks=%d requeues=%v, want exactly one Nack(requeue=true)",
			ack.nacks, ack.requeues)
	}
	if ack.acks != 0 {
		t.Fatalf("in-flight delivery was acked (%d) instead of nacked", ack.acks)
	}
}

// With AutoAck the server already considers the delivery settled at send
// time, so there is nothing to Nack — pin that asymmetry so the fix never
// "helpfully" nacks an auto-acked delivery.
func TestHandlerGoroutineSkipsNackWhenAutoAck(t *testing.T) {
	ack := &recordingAcknowledger{}
	msgs := make(chan amqp.Delivery, 1)
	msgs <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1}
	close(msgs)

	consumer := newClosedTestConsumer()
	consumer.options.RabbitConsumerOptions.AutoAck = true
	handlerGoroutine(consumer, msgs, consumer.options, func(d Delivery) Action {
		t.Error("handler must not run on a closed consumer")
		return Ack
	})

	ack.mu.Lock()
	defer ack.mu.Unlock()
	if ack.nacks != 0 {
		t.Fatalf("auto-acked delivery was nacked %d times, want 0", ack.nacks)
	}
}
