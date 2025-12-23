package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (publisher *Publisher) startNotifyFlowHandler() {
	for publisher.notifyHandlerReady() {
		flowChan := publisher.chanManager.NotifyFlowSafe(make(chan bool))
		publisher.disablePublishDueToFlowMu.Lock()
		publisher.disablePublishDueToFlow = false
		publisher.disablePublishDueToFlowMu.Unlock()

		if !publisher.handleFlowEvents(flowChan) {
			return
		}
	}
}

func (publisher *Publisher) handleFlowEvents(flowChan <-chan bool) bool {
	for {
		select {
		case <-publisher.done:
			return false
		case ok, open := <-flowChan:
			if !open {
				return true // channel closed, re-register
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

func (publisher *Publisher) startNotifyBlockedHandler() {
	for publisher.notifyHandlerReady() {
		blockings := publisher.connManager.NotifyBlockedSafe(make(chan amqp.Blocking))
		publisher.disablePublishDueToBlockedMu.Lock()
		publisher.disablePublishDueToBlocked = false
		publisher.disablePublishDueToBlockedMu.Unlock()

		if !publisher.handleBlockedEvents(blockings) {
			return
		}
	}
}

func (publisher *Publisher) handleBlockedEvents(blockings <-chan amqp.Blocking) bool {
	for {
		select {
		case <-publisher.done:
			return false
		case b, open := <-blockings:
			if !open {
				return true // channel closed, re-register
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

// notifyHandlerReady checks if the handler should proceed to register.
// Returns false if publisher is closing or connection is closed.
func (publisher *Publisher) notifyHandlerReady() bool {
	select {
	case <-publisher.done:
		return false
	default:
	}
	return !publisher.connManager.IsClosed()
}
