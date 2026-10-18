package runtime

import (
	"fmt"
	"time"
)

const (
	defaultDispatchRetryInitialBackoff = 2 * time.Second
	defaultDispatchRetryMaxBackoff     = 2 * time.Minute
	defaultDispatchRetryJitterFraction = 0.5
)

// DispatchRetryPolicy bounds how quickly the controller re-executes a
// transient dispatch failure: a watcher, transport, or leadership-guard
// error that does not prove the durable responder Job stopped, so the same
// incident effect re-runs and the idempotent dispatcher rejoins the existing
// Job. A Job Kubernetes marks failed is a different, terminal case and
// never retries through this policy.
//
// Zero fields take the defaults: exponential backoff from 2s, capped at
// 2 min, with equal jitter (delay uniform in [backoff/2, backoff]) so an
// API-server outage does not spin the dispatch effect in a hot loop, and
// several controllers recovering from the same outage do not all retry in
// lockstep.
type DispatchRetryPolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	// JitterFraction is the fraction of the computed backoff that is
	// randomized away: a retry's delay is uniform in
	// [(1-JitterFraction)*backoff, backoff]. Must be within [0, 1].
	JitterFraction float64
}

func (p DispatchRetryPolicy) withDefaults() (DispatchRetryPolicy, error) {
	if p.InitialBackoff < 0 || p.MaxBackoff < 0 {
		return p, fmt.Errorf("dispatch retry backoff must not be negative")
	}
	if p.JitterFraction < 0 || p.JitterFraction > 1 {
		return p, fmt.Errorf("dispatch retry jitter fraction must be within [0, 1], got %v", p.JitterFraction)
	}
	if p.InitialBackoff == 0 {
		p.InitialBackoff = defaultDispatchRetryInitialBackoff
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = max(defaultDispatchRetryMaxBackoff, p.InitialBackoff)
	}
	if p.MaxBackoff < p.InitialBackoff {
		return p, fmt.Errorf(
			"dispatch retry max backoff %s is below the initial backoff %s", p.MaxBackoff, p.InitialBackoff,
		)
	}
	if p.JitterFraction == 0 {
		p.JitterFraction = defaultDispatchRetryJitterFraction
	}
	return p, nil
}

// backoff returns the capped exponential delay before jitter, for the given
// number of consecutive transient dispatch failures (1-indexed).
func (p DispatchRetryPolicy) backoff(attempts int) time.Duration {
	delay := p.InitialBackoff
	for range attempts - 1 {
		if delay >= p.MaxBackoff/2 {
			return p.MaxBackoff
		}
		delay *= 2
	}
	return min(delay, p.MaxBackoff)
}

// jittered applies equal jitter to base: the result is uniform in
// [(1-p.JitterFraction)*base, base], so a cap the caller already applied to
// base is never exceeded. draw is a caller-supplied sample in [0, 1) so
// callers can keep the delay deterministic under test.
func (p DispatchRetryPolicy) jittered(base time.Duration, draw float64) time.Duration {
	spread := time.Duration(float64(base) * p.JitterFraction)
	if spread <= 0 {
		return base
	}
	return base - spread + time.Duration(draw*float64(spread))
}

// recordDispatchFailureLocked counts a transient dispatch failure and
// schedules the next retry with bounded, jittered exponential backoff.
// Callers hold c.mu.
func (c *Controller) recordDispatchFailureLocked() {
	c.dispatchFailureAttempts++
	policy := c.config.DispatchRetry
	base := policy.backoff(c.dispatchFailureAttempts)
	draw := 0.0
	if c.dispatchJitter != nil {
		draw = c.dispatchJitter()
	}
	c.dispatchNextRetryAt = c.clock().Add(policy.jittered(base, draw))
}

// resetDispatchRetryLocked clears the backoff state: a fresh incident, a
// terminal dispatch failure, or a completed dispatch all start clean.
// Callers hold c.mu.
func (c *Controller) resetDispatchRetryLocked() {
	c.dispatchFailureAttempts = 0
	c.dispatchNextRetryAt = time.Time{}
}

// dispatchRetryDueLocked reports whether a pending dispatch effect may run.
// Callers hold c.mu.
func (c *Controller) dispatchRetryDueLocked() bool {
	return c.dispatchNextRetryAt.IsZero() || !c.clock().Before(c.dispatchNextRetryAt)
}

// dispatchRetryWake returns the next dispatch retry time when it is earlier
// than next, so the runtime loop wakes to re-execute the dispatch effect
// instead of spinning or sleeping past it.
func (c *Controller) dispatchRetryWake(next time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dispatchState != "pending" || c.dispatchNextRetryAt.IsZero() {
		return next
	}
	if next.IsZero() || c.dispatchNextRetryAt.Before(next) {
		return c.dispatchNextRetryAt
	}
	return next
}
