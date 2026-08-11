package backoff

import (
	"sync"
	"testing"
	"time"
)

// cenkalti draws each delay from [0.5x, 1.5x] of the current interval, and
// the interval doubles from base to max — so every delay lies within
// [0.5*base, 1.5*max].
func TestNextStaysWithinBounds(t *testing.T) {
	base := time.Second
	max := 8 * time.Second
	b := New(base, max)
	for i := 0; i < 200; i++ {
		got := b.Next()
		if got < base/2 || got > max+max/2 {
			t.Fatalf("Next() = %v, want within [%v, %v]", got, base/2, max+max/2)
		}
	}
}

// The first delay must be drawn around base, not the cap.
func TestFirstDelayIsAroundBase(t *testing.T) {
	base := time.Second
	b := New(base, time.Minute)
	if got := b.Next(); got < base/2 || got > base+base/2 {
		t.Fatalf("first Next() = %v, want within [%v, %v]", got, base/2, base+base/2)
	}
}

// Delays escalate: within a few attempts the draws must exceed anything the
// base interval could produce.
func TestDelaysEscalateTowardsMax(t *testing.T) {
	base := 10 * time.Millisecond
	max := 10 * time.Second
	b := New(base, max)
	escalated := false
	for i := 0; i < 12; i++ {
		if b.Next() > base+base/2 {
			escalated = true
			break
		}
	}
	if !escalated {
		t.Fatal("delays never escalated beyond the base interval's range")
	}
}

// Reset must return the schedule to the base interval.
func TestResetReturnsToBase(t *testing.T) {
	base := time.Second
	b := New(base, time.Minute)
	for i := 0; i < 8; i++ {
		b.Next()
	}
	b.Reset()
	if got := b.Next(); got < base/2 || got > base+base/2 {
		t.Fatalf("Next() after Reset() = %v, want within [%v, %v]", got, base/2, base+base/2)
	}
}

// The backoff must NEVER stop: cenkalti's MaxElapsedTime default (15 min)
// makes NextBackOff return Stop (-1), and a reconnect loop waiting on
// time.After(Stop) fires immediately, turning recovery into a hot loop.
// New() must disable it.
func TestNeverReturnsStop(t *testing.T) {
	b := New(time.Millisecond, 4*time.Millisecond)
	for i := 0; i < 500; i++ {
		if got := b.Next(); got < 0 {
			t.Fatalf("Next() = %v (Stop) on attempt %d: MaxElapsedTime not disabled", got, i)
		}
	}
}

func TestMaxBelowBaseClampsToBase(t *testing.T) {
	base := 10 * time.Second
	b := New(base, time.Second)
	for i := 0; i < 5; i++ {
		if got := b.Next(); got < base/2 || got > base+base/2 {
			t.Fatalf("Next() = %v, want within [%v, %v]", got, base/2, base+base/2)
		}
	}
}

func TestNonPositiveBaseDefaults(t *testing.T) {
	b := New(0, time.Minute)
	if got := b.Next(); got < time.Second/2 || got > time.Second+time.Second/2 {
		t.Fatalf("Next() = %v, want around 1s", got)
	}
}

// Jitter must actually jitter: repeated draws at the cap may not collapse to
// a single value (the fleet-desynchronization property).
func TestNextJitters(t *testing.T) {
	b := New(time.Second, time.Hour)
	for i := 0; i < 15; i++ {
		b.Next()
	}
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 20; i++ {
		seen[b.Next()] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("expected jittered delays at the cap, got %d distinct values", len(seen))
	}
}

func TestConcurrentUse(t *testing.T) {
	base := time.Millisecond
	max := time.Second
	b := New(base, max)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if got := b.Next(); got < base/2 || got > max+max/2 {
					t.Errorf("Next() = %v, want within [%v, %v]", got, base/2, max+max/2)
				}
				if j%10 == 0 {
					b.Reset()
				}
			}
		}()
	}
	wg.Wait()
}
