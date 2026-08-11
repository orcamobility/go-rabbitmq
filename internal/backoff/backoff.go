// Package backoff adapts github.com/cenkalti/backoff/v4 for the reconnect
// loops: exponential growth with jitter from a base interval towards a cap,
// never stopping, resettable once a session has proven stable.
package backoff

import (
	"sync"
	"time"

	cenkalti "github.com/cenkalti/backoff/v4"
)

// Exponential produces exponentially growing, jittered delays. It is safe
// for concurrent use.
//
// Delays are drawn from [0.5x, 1.5x] of the current interval (cenkalti's
// RandomizationFactor 0.5), where the interval doubles per attempt until it
// reaches max — so the worst-case delay is 1.5*max. Meaningful jitter is the
// fleet-desynchronization property this exists for: INC-2026-08-04 showed
// that near-synchronized retries from ~1,300 NVRs re-trip the broker's
// memory alarm during recovery (10% jitter demonstrably was not enough).
type Exponential struct {
	mu sync.Mutex
	eb *cenkalti.ExponentialBackOff
}

// New returns an Exponential backoff from base towards max. A non-positive
// base defaults to one second; a max below base is raised to base.
func New(base, max time.Duration) *Exponential {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	eb := cenkalti.NewExponentialBackOff()
	eb.InitialInterval = base
	eb.MaxInterval = max
	eb.Multiplier = 2
	eb.RandomizationFactor = 0.5
	// Never give up: cenkalti's default MaxElapsedTime (15 min) makes
	// NextBackOff return Stop (-1) once the total elapsed time is exceeded,
	// and a reconnect loop waiting on time.After(-1) would fire immediately,
	// turning recovery into a hot loop. Recovery must keep retrying for as
	// long as the manager lives.
	eb.MaxElapsedTime = 0
	eb.Reset()
	return &Exponential{eb: eb}
}

// Next returns the delay to wait before the next attempt.
func (e *Exponential) Next() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.eb.NextBackOff()
}

// Reset restores the initial state so the next delay is drawn around base.
func (e *Exponential) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.eb.Reset()
}
