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
	case <-m.closeCh:
		return nil, amqp.ErrClosed
	}
	confirmation, err := m.publishWithConfirmedRouting(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		// A late return must never be assigned to a later publication.
		m.failedConfirm = true
		return nil, ctx.Err()
	case <-m.closeCh:
		return nil, amqp.ErrClosed
	case <-confirmation.Done():
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

func (m *ChannelManager) publishWithConfirmedRouting(
	ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	// Confirm and recovery's declarations share one AMQP reply stream.
	m.channelMu.Lock()
	defer m.channelMu.Unlock()
	select {
	case <-m.closeCh:
		return nil, amqp.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := msg.Headers.Validate(); err != nil {
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
	// Separate cancellation before send from a write with an uncertain outcome.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	confirmation, err := m.channel.PublishWithDeferredConfirm(exchange, key, mandatory, immediate, msg)
	if err != nil {
		m.failedConfirm = true
		return nil, err
	}
	return confirmation, nil
}
