package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

func prepareDockerTestWithContainerID(t *testing.T) (connStr string, containerID string) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach", "--publish=5672:5672", "--quiet", "--", "rabbitmq:4.1.1-alpine").Output()
	if err != nil {
		t.Log("container id", string(out))
		t.Fatalf("error launching rabbitmq in docker: %v", err)
	}
	containerID = strings.TrimSpace(string(out))
	t.Cleanup(func() {
		t.Logf("attempting to shutdown container '%s'", containerID)
		if err := exec.Command("docker", "rm", "--force", containerID).Run(); err != nil {
			t.Logf("failed to stop: %v", err)
		}
	})
	return "amqp://guest:guest@localhost:5672/", containerID
}

// waitForGoroutineCount polls until the goroutine count drops to at most
// target, or the deadline expires. Returns the final goroutine count.
func waitForGoroutineCount(t *testing.T, target int, deadline time.Duration) int {
	t.Helper()
	timer := time.After(deadline)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer:
			return runtime.NumGoroutine()
		case <-ticker.C:
			if current := runtime.NumGoroutine(); current <= target {
				return current
			}
		}
	}
}

// TestPublisherGoroutineCleanup verifies that all goroutines spawned by NewPublisher
// are cleaned up after Close() is called. This catches goroutine leaks in the
// flow/blocked notification handlers.
func TestPublisherGoroutineCleanup(t *testing.T) {
	connStr := prepareDockerTest(t)
	conn := waitForHealthyAmqp(t, connStr)

	// Allow background goroutines from connection setup to stabilize
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()
	t.Logf("baseline goroutine count: %d", baseline)

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	withPublisher := runtime.NumGoroutine()
	t.Logf("goroutine count with publisher: %d (delta: +%d)", withPublisher, withPublisher-baseline)

	if withPublisher <= baseline {
		t.Fatal("expected goroutine count to increase after creating publisher")
	}

	// Cleanup: publisher first (signals done to handlers), then connection
	publisher.Close()
	conn.Close()

	// Wait for goroutines to drain back to near baseline.
	// Tolerance of 2 accounts for runtime variance (GC finalizers, timers).
	final := waitForGoroutineCount(t, baseline+2, 5*time.Second)
	t.Logf("goroutine count after cleanup: %d (delta from baseline: +%d)", final, final-baseline)

	if final > baseline+2 {
		t.Fatalf("goroutine leak detected: baseline=%d, after close=%d (delta: +%d)",
			baseline, final, final-baseline)
	}
}

// TestPublisherGoroutineStableAcrossReconnections verifies that goroutines do not
// accumulate across multiple reconnections. Before the self-healing fix, each
// reconnection would spawn new flow/blocked handler goroutines without cleaning
// up the old ones.
func TestPublisherGoroutineStableAcrossReconnections(t *testing.T) {
	connStr, containerID := prepareDockerTestWithContainerID(t)
	conn := waitForHealthyAmqp(t, connStr)

	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("error creating publisher: %v", err)
	}

	// Let goroutines stabilize and record count with publisher active
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	withPublisher := runtime.NumGoroutine()
	t.Logf("goroutine count with publisher: %d", withPublisher)

	// Force 3 reconnections via rabbitmqctl
	for i := 0; i < 3; i++ {
		t.Logf("forcing reconnection %d/3", i+1)
		closeCmd := exec.Command("docker", "exec", containerID,
			"rabbitmqctl", "close_all_connections", "test-reconnect")
		if out, err := closeCmd.CombinedOutput(); err != nil {
			t.Logf("rabbitmqctl output: %s, err: %v", string(out), err)
		}
		// Wait for reconnection (default reconnect interval is 5s + buffer)
		time.Sleep(8 * time.Second)
	}

	// After 3 reconnections, goroutine count should not have grown significantly
	runtime.GC()
	time.Sleep(time.Second)
	afterReconnect := runtime.NumGoroutine()
	t.Logf("goroutine count after 3 reconnections: %d (delta: %+d)", afterReconnect, afterReconnect-withPublisher)

	// Tolerance of 2 for runtime variance
	if afterReconnect > withPublisher+2 {
		t.Errorf("goroutine leak across reconnections: before=%d, after=%d (delta: +%d)",
			withPublisher, afterReconnect, afterReconnect-withPublisher)
	}

	// Final cleanup
	publisher.Close()
	conn.Close()
}
