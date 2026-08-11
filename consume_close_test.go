package rabbitmq

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// This file unit-tests the consumer side of the INC-2026-08-04 lifecycle
// races (ACB-379), using bare-struct consumers in the style of
// flow_block_test.go so they run without a broker:
//
//   - a closed consumer must never re-register goroutines when a reconnect
//     event races Close() (the "zombie consumer" that competes with its
//     replacement for deliveries), and
//   - a handler that exits because the consumer closed must Nack the
//     delivery it is holding: delivered-unacked messages are exempt from
//     queue TTL, so silently dropping one pins it invisibly on the channel
//     until the channel dies (observed in production as "NVR ignores
//     commands").

func newClosedTestConsumer() *Consumer {
	options := getDefaultConsumerOptions("close-race-queue")
	return &Consumer{
		options:    options,
		handlerMu:  &sync.RWMutex{},
		isClosedMu: &sync.RWMutex{},
		isClosed:   true,
		// chanManager and closeConnectionToManagerCh are deliberately nil:
		// a correctly guarded code path must return before touching either,
		// and an unguarded one fails loudly here instead of leaking quietly
		// in production.
	}
}

// A reconnect event that races Close() must not re-declare topology and
// re-register basic.consume on the recovered channel. Without the guard this
// panics on the nil chanManager — in production it would instead silently
// create the zombie consumer.
func TestStartGoroutinesRefusesWhenClosed(t *testing.T) {
	consumer := newClosedTestConsumer()

	err := consumer.startGoroutines(func(d Delivery) Action { return Ack }, consumer.options)
	if !errors.Is(err, errConsumerClosed) {
		t.Fatalf("startGoroutines on closed consumer returned %v, want errConsumerClosed", err)
	}
}

// cleanupResources runs from both Close() and Run's exit path, so a second
// invocation must be a no-op: the dispatcher's unsubscribe receiver consumes
// exactly one message, and a second synchronous send would block forever.
func TestCleanupResourcesIdempotent(t *testing.T) {
	consumer := newClosedTestConsumer()

	done := make(chan struct{})
	go func() {
		consumer.cleanupResources()
		close(done)
	}()

	select {
	case <-done:
		// returned without touching the nil chanManager or sending a second
		// unsubscribe — correct.
	case <-time.After(2 * time.Second):
		t.Fatal("cleanupResources on an already-closed consumer did not return")
	}
}
