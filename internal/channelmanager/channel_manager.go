package channelmanager

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq/internal/backoff"
	"github.com/wagslane/go-rabbitmq/internal/connectionmanager"
	"github.com/wagslane/go-rabbitmq/internal/dispatcher"
	"github.com/wagslane/go-rabbitmq/internal/logger"
)

// ChannelManager -
type ChannelManager struct {
	logger              logger.Logger
	channel             *amqp.Channel
	connManager         *connectionmanager.ConnectionManager
	channelMu           *sync.RWMutex
	connectedAt         time.Time
	stableAfter         time.Duration
	reconnectBackoff    *backoff.Exponential
	reconnectionCount   uint
	reconnectionCountMu *sync.Mutex
	dispatcher          *dispatcher.Dispatcher
	closeCh             chan struct{}
	closeOnce           sync.Once
}

// NewChannelManager creates a new connection manager
func NewChannelManager(connManager *connectionmanager.ConnectionManager, log logger.Logger, reconnectInterval, reconnectMaxInterval time.Duration) (*ChannelManager, error) {
	ch, err := getNewChannel(connManager)
	if err != nil {
		return nil, err
	}

	chanManager := ChannelManager{
		logger:              log,
		connManager:         connManager,
		channel:             ch,
		channelMu:           &sync.RWMutex{},
		connectedAt:         time.Now(),
		stableAfter:         reconnectMaxInterval,
		reconnectBackoff:    backoff.New(reconnectInterval, reconnectMaxInterval),
		reconnectionCount:   0,
		reconnectionCountMu: &sync.Mutex{},
		dispatcher:          dispatcher.NewDispatcher(),
		closeCh:             make(chan struct{}),
	}
	go chanManager.startNotifyCancelOrClosed()
	return &chanManager, nil
}

func getNewChannel(connManager *connectionmanager.ConnectionManager) (*amqp.Channel, error) {
	conn := connManager.CheckoutConnection()
	defer connManager.CheckinConnection()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chanManager *ChannelManager) startNotifyCancelOrClosed() {
	notifyCloseChan := chanManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := chanManager.channel.NotifyCancel(make(chan string, 1))

	notification := waitForChannelNotification(notifyCloseChan, notifyCancelChan)
	if !notification.cancelled {
		err := notification.closeErr
		if err != nil {
			chanManager.logger.Errorf("attempting to reconnect to amqp server after close with error: %v", err)
			chanManager.resetBackoffIfStable()
			if !chanManager.reconnectLoop() {
				return
			}
			chanManager.logger.Warnf("successfully reconnected to amqp server")
			chanManager.dispatcher.Dispatch(err)
		}
		if err == nil {
			chanManager.logger.Infof("amqp channel closed gracefully")
		}
		return
	}

	chanManager.logger.Errorf("attempting to reconnect to amqp server after cancel with error: %s", notification.cancelTag)
	chanManager.resetBackoffIfStable()
	if !chanManager.reconnectLoop() {
		return
	}
	chanManager.logger.Warnf("successfully reconnected to amqp server after cancel")
	chanManager.dispatcher.Dispatch(errors.New(notification.cancelTag))
}

type channelNotification struct {
	closeErr  *amqp.Error
	cancelTag string
	cancelled bool
}

func waitForChannelNotification(notifyClose <-chan *amqp.Error, notifyCancel <-chan string) channelNotification {
	select {
	case err := <-notifyClose:
		return channelNotification{closeErr: err}
	case tag, ok := <-notifyCancel:
		if ok {
			return channelNotification{cancelTag: tag, cancelled: true}
		}
		// amqp091-go closes NotifyClose before NotifyCancel on every channel
		// shutdown. Preserve an abnormal close error when both are ready instead
		// of randomly treating the closed cancel notifier as graceful shutdown.
		return channelNotification{closeErr: <-notifyClose}
	}
}

// GetReconnectionCount -
func (chanManager *ChannelManager) GetReconnectionCount() uint {
	chanManager.reconnectionCountMu.Lock()
	defer chanManager.reconnectionCountMu.Unlock()
	return chanManager.reconnectionCount
}

func (chanManager *ChannelManager) incrementReconnectionCount() {
	chanManager.reconnectionCountMu.Lock()
	defer chanManager.reconnectionCountMu.Unlock()
	chanManager.reconnectionCount++
}

var errManagerClosed = errors.New("channel manager is closed")

// reconnectLoop continuously attempts to reconnect until it succeeds or the
// manager is closed. Returns whether a new channel was installed.
func (chanManager *ChannelManager) reconnectLoop() bool {
	for {
		wait := chanManager.reconnectBackoff.Next()
		chanManager.logger.Infof("waiting %s to attempt to reconnect to amqp server", wait)
		select {
		case <-chanManager.closeCh:
			chanManager.logger.Infof("channel manager closed, stopping reconnect loop")
			return false
		case <-time.After(wait):
		}
		err := chanManager.reconnect()
		if errors.Is(err, errManagerClosed) {
			chanManager.logger.Infof("channel manager closed, stopping reconnect loop")
			return false
		}
		if err != nil {
			chanManager.logger.Errorf("error reconnecting to amqp server: %v", err)
			continue
		}
		select {
		case <-chanManager.closeCh:
			// Close raced the reconnect: release the channel we just opened so
			// it doesn't hold a channel id on the connection forever.
			chanManager.channelMu.Lock()
			if err := chanManager.channel.Close(); err != nil {
				chanManager.logger.Warnf("error closing channel after close raced reconnect: %v", err)
			}
			chanManager.channelMu.Unlock()
			return false
		default:
		}
		chanManager.incrementReconnectionCount()
		go chanManager.startNotifyCancelOrClosed()
		return true
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chanManager *ChannelManager) reconnect() error {
	chanManager.channelMu.Lock()
	defer chanManager.channelMu.Unlock()

	select {
	case <-chanManager.closeCh:
		return errManagerClosed
	default:
	}

	if chanManager.channel != nil {
		if err := chanManager.channel.Close(); err != nil {
			chanManager.logger.Warnf("error closing channel while reconnecting: %v", err)
		}
	}

	newChannel, err := getNewChannel(chanManager.connManager)
	if err != nil {
		return err
	}

	chanManager.channel = newChannel
	chanManager.connectedAt = time.Now()
	return nil
}

func (chanManager *ChannelManager) resetBackoffIfStable() {
	chanManager.channelMu.RLock()
	stable := time.Since(chanManager.connectedAt) >= chanManager.stableAfter
	chanManager.channelMu.RUnlock()
	if stable {
		chanManager.reconnectBackoff.Reset()
	}
}

// Close safely closes the current channel and stops any reconnect loop
func (chanManager *ChannelManager) Close() error {
	chanManager.logger.Infof("closing channel manager...")
	chanManager.closeOnce.Do(func() { close(chanManager.closeCh) })
	chanManager.channelMu.Lock()
	defer chanManager.channelMu.Unlock()

	err := chanManager.channel.Close()
	if err != nil {
		return err
	}

	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnect to the server
func (chanManager *ChannelManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return chanManager.dispatcher.AddSubscriber()
}
