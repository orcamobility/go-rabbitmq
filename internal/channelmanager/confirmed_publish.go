package channelmanager

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ReturnedError struct{ amqp.Return }

func (e *ReturnedError) Error() string {
	return fmt.Sprintf("message returned from exchange %q with routing key %q: %d %s", e.Exchange, e.RoutingKey, e.ReplyCode, e.ReplyText)
}

func (m *ChannelManager) PublishWithConfirmedRoutingWithContextSafe(
	ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	select {
	case m.confirmedPublish <- struct{}{}:
		defer func() { <-m.confirmedPublish }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	m.channelMu.RLock()
	defer m.channelMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.returnChannel != m.channel {
		if err := m.channel.Confirm(false); err != nil {
			return nil, err
		}
		m.returns = m.channel.NotifyReturn(make(chan amqp.Return, 1))
		m.returnChannel = m.channel
		m.failedConfirm = false
	}
	if m.failedConfirm {
		return nil, amqp.ErrClosed
	}
	confirmation, err := m.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		m.failedConfirm = true
		return nil, err
	}
	if _, err = confirmation.WaitContext(ctx); err != nil {
		// A late return must never be assigned to a later publication.
		m.failedConfirm = true
		return nil, err
	}
	// AMQP dispatches basic.return before its confirm. This channel is filled
	// directly by that reader, without an asynchronous callback in between.
	select {
	case returned, ok := <-m.returns:
		if ok {
			return nil, &ReturnedError{Return: returned}
		}
	default:
	}
	return confirmation, nil
}
