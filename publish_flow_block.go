package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (publisher *Publisher) startNotifyFlowHandler() {
	for {
		select {
		case <-publisher.done:
			return
		default:
		}
		if publisher.connManager.IsClosed() {
			return
		}

		notifyFlowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool))
		publisher.disablePublishDueToFlowMu.Lock()
		publisher.disablePublishDueToFlow = false
		publisher.disablePublishDueToFlowMu.Unlock()

	flowLoop:
		for {
			select {
			case <-publisher.done:
				return
			case ok, open := <-notifyFlowChan:
				if !open {
					break flowLoop
				}
				publisher.disablePublishDueToFlowMu.Lock()
				if ok {
					publisher.options.Logger.Warnf("pausing publishing due to flow request from server")
					publisher.disablePublishDueToFlow = true
				} else {
					publisher.disablePublishDueToFlow = false
					publisher.options.Logger.Warnf("resuming publishing due to flow request from server")
				}
				publisher.disablePublishDueToFlowMu.Unlock()
			}
		}
	}
}

func (publisher *Publisher) startNotifyBlockedHandler() {
	for {
		// Check if closed before (re-)registering
		select {
		case <-publisher.done:
			return
		default:
		}
		if publisher.connManager.IsClosed() {
			return
		}

		blockings := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking))
		publisher.disablePublishDueToBlockedMu.Lock()
		publisher.disablePublishDueToBlocked = false
		publisher.disablePublishDueToBlockedMu.Unlock()

	blockedLoop:
		for {
			select {
			case <-publisher.done:
				return
			case b, open := <-blockings:
				if !open {
					break blockedLoop
				}
				publisher.disablePublishDueToBlockedMu.Lock()
				if b.Active {
					publisher.options.Logger.Warnf("pausing publishing due to TCP blocking from server")
					publisher.disablePublishDueToBlocked = true
				} else {
					publisher.disablePublishDueToBlocked = false
					publisher.options.Logger.Warnf("resuming publishing due to TCP blocking from server")
				}
				publisher.disablePublishDueToBlockedMu.Unlock()
			}
		}
	}
}
