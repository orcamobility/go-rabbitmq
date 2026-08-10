package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// This file regression-tests the Close()-vs-reconnectLoop race behind
// INC-2026-08-04 (ACB-379): closing a consumer or a connection while its
// manager is mid-outage in its reconnect loop must NOT let that loop later
// succeed and leave behind a live channel/connection that nothing owns,
// uses, or ever closes.
//
// The tests need to stop and restart the broker, so they run their own
// restartable container (no --rm, and a dedicated host port so they cannot
// collide with prepareDockerTest's broker) and count server-side objects
// with rabbitmqctl inside the container, which works on the plain
// non-management image.

const restartableBrokerHostPort = "5674"

func prepareRestartableDockerTest(t *testing.T) (connStr string, containerID string) {
	t.Helper()
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return "", ""
	}
	out, err := exec.Command(
		"docker", "run", "--detach",
		"--publish="+restartableBrokerHostPort+":5672",
		"--quiet", "--", "rabbitmq:4.1.1-alpine",
	).Output()
	if err != nil {
		t.Fatalf("error launching rabbitmq in docker: %v", err)
	}
	containerID = strings.TrimSpace(string(out))
	t.Cleanup(func() {
		if err := exec.Command("docker", "rm", "--force", containerID).Run(); err != nil {
			t.Logf("failed to remove container %s: %v", containerID, err)
		}
	})
	waitForBrokerReady(t, containerID)
	return "amqp://guest:guest@localhost:" + restartableBrokerHostPort + "/", containerID
}

func waitForBrokerReady(t *testing.T, containerID string) {
	t.Helper()
	// await_startup blocks until the broker is ready, but errors outright if
	// the Erlang VM has not come up yet — so retry it until the deadline.
	// The deadline is generous because `go test ./...` runs packages in
	// parallel, and several broker containers booting at once (plus the race
	// detector) can slow a single startup well past its usual few seconds.
	deadline := time.Now().Add(180 * time.Second)
	var lastOut []byte
	var lastErr error
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "exec", "--user", "rabbitmq", containerID, "rabbitmqctl", "await_startup")
		lastOut, lastErr = cmd.CombinedOutput()
		if lastErr == nil {
			return
		}
		time.Sleep(time.Second)
	}
	if logs, err := exec.Command("docker", "logs", "--tail", "40", containerID).CombinedOutput(); err == nil {
		t.Logf("broker container logs:\n%s", logs)
	}
	t.Fatalf("broker did not become ready: %v\n%s", lastErr, lastOut)
}

func stopBroker(t *testing.T, containerID string) {
	t.Helper()
	if out, err := exec.Command("docker", "stop", containerID).CombinedOutput(); err != nil {
		t.Fatalf("failed to stop broker: %v\n%s", err, out)
	}
}

func startBroker(t *testing.T, containerID string) {
	t.Helper()
	if out, err := exec.Command("docker", "start", containerID).CombinedOutput(); err != nil {
		t.Fatalf("failed to start broker: %v\n%s", err, out)
	}
	waitForBrokerReady(t, containerID)
}

// countBrokerObjects returns the number of rows reported by
// `rabbitmqctl list_<what>` inside the broker container.
func countBrokerObjects(t *testing.T, containerID, what string) int {
	t.Helper()
	out, err := exec.Command(
		"docker", "exec", "--user", "rabbitmq", containerID,
		"rabbitmqctl", "list_"+what, "--quiet", "--no-table-headers",
	).Output()
	if err != nil {
		t.Fatalf("rabbitmqctl list_%s failed: %v", what, err)
	}
	count := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// waitForBrokerObjects polls until the broker reports exactly want objects,
// failing the test if that does not happen within the deadline.
func waitForBrokerObjects(t *testing.T, containerID, what string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		got = countBrokerObjects(t, containerID, what)
		if got == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out: broker reports %d %s, want %d", got, what, want)
}

// assertBrokerObjectsStayAt polls for dur and fails as soon as the count
// diverges from want. On the unfixed code the leaked object appears within a
// couple of reconnect intervals of the broker recovering, so a divergence
// check turns the leak into a deterministic test failure.
func assertBrokerObjectsStayAt(t *testing.T, containerID, what string, want int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := countBrokerObjects(t, containerID, what); got != want {
			t.Fatalf("leak detected: broker reports %d %s, want %d", got, what, want)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestConsumerCloseDuringOutageDoesNotLeakChannel is the ACB-379 regression
// test for the channel-level race:
//
//	broker outage -> consumer's ChannelManager enters reconnectLoop
//	-> Consumer.Close() during the outage
//	-> broker returns, shared connection recovers
//	-> the closed consumer's loop must NOT open a channel nobody owns.
//
// On the unfixed code the orphan channel appears within one or two reconnect
// intervals of the connection recovering, and is self-healing from then on.
func TestConsumerCloseDuringOutageDoesNotLeakChannel(t *testing.T) {
	connStr, containerID := prepareRestartableDockerTest(t)

	conn, err := NewConn(connStr,
		WithConnectionOptionsLogger(simpleLogF(t.Logf)),
		WithConnectionOptionsReconnectInterval(500*time.Millisecond),
	)
	if err != nil {
		t.Fatal("error creating connection", err)
	}
	defer conn.Close()

	consumer, err := NewConsumer(conn, "close_race_queue",
		WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
	}()

	// One consumer owns exactly one channel.
	waitForBrokerObjects(t, containerID, "channels", 1)

	// Broker outage: the consumer's channel manager is now cycling
	// sleep -> dial-fail -> sleep. Closing the consumer NOW is the
	// INC-2026-08-04 interleaving.
	stopBroker(t, containerID)
	time.Sleep(2 * time.Second)

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumer.CloseWithContext(closeCtx)

	// Broker returns; the shared connection self-heals.
	startBroker(t, containerID)
	waitForBrokerObjects(t, containerID, "connections", 1)

	// A closed consumer must stay closed: no channel may reappear.
	assertBrokerObjectsStayAt(t, containerID, "channels", 0, 6*time.Second)
}

// TestConnCloseDuringOutageDoesNotLeakConnection is the connection-level
// variant of the same race: Conn.Close() during an outage must stop the
// ConnectionManager's reconnect loop, not leave behind a loop that later
// dials a TCP connection nothing owns.
func TestConnCloseDuringOutageDoesNotLeakConnection(t *testing.T) {
	connStr, containerID := prepareRestartableDockerTest(t)

	conn, err := NewConn(connStr,
		WithConnectionOptionsLogger(simpleLogF(t.Logf)),
		WithConnectionOptionsReconnectInterval(500*time.Millisecond),
	)
	if err != nil {
		t.Fatal("error creating connection", err)
	}

	waitForBrokerObjects(t, containerID, "connections", 1)

	// Broker outage: the ConnectionManager is now cycling
	// sleep -> dial-fail -> sleep. Closing the connection NOW races its
	// reconnect loop.
	stopBroker(t, containerID)
	time.Sleep(2 * time.Second)

	if err := conn.Close(); err != nil {
		// The current connection is already dead; an error from closing it
		// is expected. The point is what happens after the broker returns.
		t.Logf("conn.Close() during outage returned: %v", err)
	}

	startBroker(t, containerID)

	// A closed connection must stay closed: nothing may redial.
	assertBrokerObjectsStayAt(t, containerID, "connections", 0, 6*time.Second)
}
