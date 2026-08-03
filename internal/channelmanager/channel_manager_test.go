package channelmanager

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

func newTestChannelManager() *ChannelManager {
	return &ChannelManager{
		logger:              nopLogger{},
		channelMu:           &sync.RWMutex{},
		reconnectInterval:   time.Hour,
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
