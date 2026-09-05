package channelmanager

import (
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/backoff"
)

type nopLogger struct{}

func (nopLogger) Fatalf(string, ...interface{}) {}
func (nopLogger) Errorf(string, ...interface{}) {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Debugf(string, ...interface{}) {}

func newTestChannelManager() *ChannelManager {
	return newTestChannelManagerWithBackoff(time.Hour, time.Hour)
}

func newTestChannelManagerWithBackoff(base, max time.Duration) *ChannelManager {
	return &ChannelManager{
		logger:              nopLogger{},
		channelMu:           &sync.RWMutex{},
		connectedAt:         time.Now(),
		stableAfter:         max,
		reconnectBackoff:    backoff.New(base, max),
		reconnectionCountMu: &sync.Mutex{},
		closeCh:             make(chan struct{}),
	}
}

// A closed manager's reconnectLoop must exit instead of retrying forever
// (each retry leaks a channel id on the shared connection).
func TestReconnectLoopStopsOnClose(t *testing.T) {
	chanManager := newTestChannelManager()

	done := make(chan bool, 1)
	go func() {
		done <- chanManager.reconnectLoop()
	}()

	chanManager.closeOnce.Do(func() { close(chanManager.closeCh) })

	select {
	case reconnected := <-done:
		if reconnected {
			t.Error("reconnectLoop reported a reconnect after close")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnectLoop did not stop after close")
	}

	if got := chanManager.GetReconnectionCount(); got != 0 {
		t.Errorf("reconnection count = %d, want 0", got)
	}
}

// reconnect on a closed manager must refuse to open a new channel.
func TestReconnectRefusesWhenClosed(t *testing.T) {
	chanManager := newTestChannelManager()
	chanManager.closeOnce.Do(func() { close(chanManager.closeCh) })

	if err := chanManager.reconnect(); !errors.Is(err, errManagerClosed) {
		t.Errorf("reconnect error = %v, want errManagerClosed", err)
	}
}

func TestWaitForChannelNotificationPreservesAbnormalClose(t *testing.T) {
	for i := 0; i < 1000; i++ {
		want := &amqp.Error{Code: 501, Reason: "connection reset"}
		notifyClose := make(chan *amqp.Error, 1)
		notifyClose <- want
		close(notifyClose)
		notifyCancel := make(chan string)
		close(notifyCancel)

		got := waitForChannelNotification(notifyClose, notifyCancel)
		if got.cancelled || got.closeErr != want {
			t.Fatalf("notification = %+v, want abnormal close %v", got, want)
		}
	}
}

// A channel that had been up longer than stableAfter is treated as healthy, so
// the next outage retries promptly from the base interval.
func TestResetBackoffIfStableResetsAfterStableChannel(t *testing.T) {
	base := 10 * time.Millisecond
	chanManager := newTestChannelManagerWithBackoff(base, time.Second)
	for i := 0; i < 6; i++ {
		chanManager.reconnectBackoff.Next()
	}
	chanManager.connectedAt = time.Now().Add(-2 * time.Second)

	chanManager.resetBackoffIfStable()

	// cenkalti jitters each draw within [0.5x, 1.5x] of the interval, so
	// "reset to base" means the next draw is around base, not exactly base.
	if got := chanManager.reconnectBackoff.Next(); got < base/2 || got > base+base/2 {
		t.Errorf("next wait after reset = %v, want within [%v, %v]", got, base/2, base+base/2)
	}
}

// A channel that dies almost immediately keeps the escalated interval, so a
// flapping broker is not hammered at the base rate by the whole fleet.
func TestResetBackoffIfStableKeepsEscalationWhenFlapping(t *testing.T) {
	base := 10 * time.Millisecond
	max := time.Second
	chanManager := newTestChannelManagerWithBackoff(base, max)
	for i := 0; i < 6; i++ {
		chanManager.reconnectBackoff.Next()
	}
	chanManager.connectedAt = time.Now()

	chanManager.resetBackoffIfStable()

	escalated := false
	for i := 0; i < 50; i++ {
		if chanManager.reconnectBackoff.Next() > base+base/2 {
			escalated = true
			break
		}
	}
	if !escalated {
		t.Errorf("backoff reset to base %v despite a flapping channel", base)
	}
}

func TestWaitForChannelNotificationGracefulCloseAndCancel(t *testing.T) {
	closed := make(chan *amqp.Error)
	close(closed)
	cancelled := make(chan string)
	close(cancelled)
	if got := waitForChannelNotification(closed, cancelled); got.cancelled || got.closeErr != nil {
		t.Fatalf("graceful close = %+v", got)
	}
	live := make(chan *amqp.Error)
	tags := make(chan string, 1)
	tags <- "consumer-tag"
	if got := waitForChannelNotification(live, tags); !got.cancelled || got.cancelTag != "consumer-tag" {
		t.Fatalf("broker cancel = %+v", got)
	}
}
