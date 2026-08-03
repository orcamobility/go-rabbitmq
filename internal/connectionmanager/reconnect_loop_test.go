package connectionmanager

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type nopLogger struct{}

func (nopLogger) Fatalf(string, ...interface{}) {}
func (nopLogger) Errorf(string, ...interface{}) {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Debugf(string, ...interface{}) {}

func newTestConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		logger:              nopLogger{},
		connectionMu:        &sync.RWMutex{},
		ReconnectInterval:   time.Hour,
		reconnectionCountMu: &sync.Mutex{},
		closeCh:             make(chan struct{}),
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
