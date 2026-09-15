package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConfirmedRouting(t *testing.T) {
	address := os.Getenv("TEST_AMQP_URL")
	if address == "" {
		t.Skip("TEST_AMQP_URL is required")
	}
	conn, err := NewConn(address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	admin, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ch, err := admin.Channel()
	if err != nil {
		t.Fatal(err)
	}
	exchange := fmt.Sprintf("confirmed-routing-%d", time.Now().UnixNano())
	p, err := NewPublisher(conn, WithPublisherOptionsExchangeName(exchange), WithPublisherOptionsExchangeDeclare,
		WithPublisherOptionsExchangeKind("direct"), WithPublisherOptionsConfirm)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	defer ch.ExchangeDelete(exchange, false, false)
	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(queue.Name, "routed", exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	options := []func(*PublishOptions){WithPublishOptionsExchange(exchange), WithPublishOptionsMandatory}
	t.Run("invalid_headers_then_success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		invalid := append(append([]func(*PublishOptions){}, options...), WithPublishOptionsHeaders(Table{"bad": make(chan int)}))
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("invalid"), []string{"routed"}, invalid...)
		if err == nil || errors.Is(err, amqp.ErrClosed) {
			t.Fatalf("want header validation error, got %v", err)
		}
		confirmations, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("valid"), []string{"routed"}, options...)
		if err != nil || len(confirmations) != 1 || !confirmations[0].Acked() {
			t.Fatalf("valid message after invalid headers was not confirmed: %v", err)
		}
		msg, ok, err := ch.Get(queue.Name, true)
		if err != nil || !ok || string(msg.Body) != "valid" {
			t.Fatalf("want only valid message, got %v, %t, %q", err, ok, msg.Body)
		}
		if _, ok, err := ch.Get(queue.Name, true); err != nil || ok {
			t.Fatalf("invalid message reached queue: %v, %t", err, ok)
		}
	})
	for i := 0; i < 30; i++ {
		t.Run(fmt.Sprintf("return_then_success_%d", i), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("unroutable"), []string{"absent"}, options...)
			var returned *ReturnedError
			if !errors.As(err, &returned) || returned.ReplyCode != 312 {
				t.Fatalf("want NO_ROUTE, got %v", err)
			}
			body := fmt.Sprintf("routed-%d", i)
			confirmations, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte(body), []string{"routed"}, options...)
			if err != nil {
				t.Fatal(err)
			}
			if len(confirmations) != 1 || !confirmations[0].Acked() {
				t.Fatalf("want one ACK, got %v", confirmations)
			}
			msg, ok, err := ch.Get(queue.Name, true)
			if err != nil || !ok || string(msg.Body) != body {
				t.Fatalf("wrong delivery: %v, %t, %q", err, ok, msg.Body)
			}
		})
	}
	t.Run("cancelled_before_publish", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := p.PublishWithConfirmedRoutingWithContext(ctx, []byte("cancelled"), []string{"routed"}, options...)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancellation, got %v", err)
		}
		_, ok, err := ch.Get(queue.Name, true)
		if err != nil || ok {
			t.Fatalf("cancelled request was published: %v, %t", err, ok)
		}
	})
}
