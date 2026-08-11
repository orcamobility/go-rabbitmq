package connectionmanager

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wagslane/go-rabbitmq/internal/backoff"
)

type nopLogger struct{}

func (nopLogger) Fatalf(string, ...interface{}) {}
func (nopLogger) Errorf(string, ...interface{}) {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Debugf(string, ...interface{}) {}

func newTestConnectionManager() *ConnectionManager {
	return newTestConnectionManagerWithBackoff(time.Hour, time.Hour)
}

func newTestConnectionManagerWithBackoff(base, max time.Duration) *ConnectionManager {
	return &ConnectionManager{
		logger:               nopLogger{},
		connectionMu:         &sync.RWMutex{},
		connectedAt:          time.Now(),
		ReconnectInterval:    base,
		ReconnectMaxInterval: max,
		reconnectBackoff:     backoff.New(base, max),
		reconnectionCountMu:  &sync.Mutex{},
		closeCh:              make(chan struct{}),
	}
}

// A closed manager's reconnectLoop must exit instead of redialing forever.
func TestReconnectLoopStopsOnClose(t *testing.T) {
	connManager := newTestConnectionManager()

	done := make(chan bool, 1)
	go func() {
		done <- connManager.reconnectLoop()
	}()

	connManager.closeOnce.Do(func() { close(connManager.closeCh) })

	select {
	case reconnected := <-done:
		if reconnected {
			t.Error("reconnectLoop reported a reconnect after close")
		}
	case <-time.After(time.Second):
		t.Fatal("reconnectLoop did not stop after close")
	}

	if got := connManager.GetReconnectionCount(); got != 0 {
		t.Errorf("reconnection count = %d, want 0", got)
	}
}

// reconnect on a closed manager must refuse to dial a new connection.
func TestReconnectRefusesWhenClosed(t *testing.T) {
	connManager := newTestConnectionManager()
	connManager.closeOnce.Do(func() { close(connManager.closeCh) })

	if err := connManager.reconnect(); !errors.Is(err, errManagerClosed) {
		t.Errorf("reconnect error = %v, want errManagerClosed", err)
	}
}

// A connection that had been up longer than the max interval is treated as
// healthy, so the next outage retries promptly from the base interval.
func TestResetBackoffIfStableResetsAfterStableConnection(t *testing.T) {
	base := 10 * time.Millisecond
	connManager := newTestConnectionManagerWithBackoff(base, time.Second)
	for i := 0; i < 6; i++ {
		connManager.reconnectBackoff.Next()
	}
	connManager.connectedAt = time.Now().Add(-2 * time.Second)

	connManager.resetBackoffIfStable()

	// cenkalti jitters each draw within [0.5x, 1.5x] of the interval, so
	// "reset to base" means the next draw is around base, not exactly base.
	if got := connManager.reconnectBackoff.Next(); got < base/2 || got > base+base/2 {
		t.Errorf("next wait after reset = %v, want within [%v, %v]", got, base/2, base+base/2)
	}
}

// A connection that drops almost immediately keeps the escalated interval, so a
// flapping broker is not redialed at the base rate by the whole fleet.
func TestResetBackoffIfStableKeepsEscalationWhenFlapping(t *testing.T) {
	base := 10 * time.Millisecond
	connManager := newTestConnectionManagerWithBackoff(base, time.Second)
	for i := 0; i < 6; i++ {
		connManager.reconnectBackoff.Next()
	}
	connManager.connectedAt = time.Now()

	connManager.resetBackoffIfStable()

	escalated := false
	for i := 0; i < 50; i++ {
		if connManager.reconnectBackoff.Next() > base+base/2 {
			escalated = true
			break
		}
	}
	if !escalated {
		t.Errorf("backoff reset to base %v despite a flapping connection", base)
	}
}
