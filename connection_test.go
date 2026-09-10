package rabbitmq

import (
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRejectAutomaticRecovery(t *testing.T) {
	// Reject before dialing, including through the shared cluster constructor.
	_, err := NewConn("invalid", WithConnectionOptionsConfig(Config{Recovery: &amqp.Recovery{}}))
	if err == nil || !strings.Contains(err.Error(), "automatic recovery is not supported") {
		t.Fatalf("expected recovery configuration error, got %v", err)
	}
}
