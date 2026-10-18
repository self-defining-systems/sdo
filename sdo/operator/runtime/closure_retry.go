package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultClosureRetryMaxAttempts    = 8
	defaultClosureRetryInitialBackoff = 2 * time.Second
	defaultClosureRetryMaxBackoff     = time.Minute

	// closureStateFailed is terminal: the broker rejected the closure
	// MaxAttempts times, so the controller stops resubmitting it.
	closureStateFailed = "failed"
)

// ClosureRetryPolicy bounds how often the controller resubmits an incident
// closure the broker rejected. Zero fields take the defaults: 8 attempts with
// exponential backoff from 2 s, capped at 1 min (about 4.5 min in total).
type ClosureRetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (p ClosureRetryPolicy) withDefaults() (ClosureRetryPolicy, error) {
	if p.MaxAttempts < 0 {
		return p, fmt.Errorf("closure retry max attempts must not be negative, got %d", p.MaxAttempts)
	}
	if p.InitialBackoff < 0 || p.MaxBackoff < 0 {
		return p, fmt.Errorf("closure retry backoff must not be negative")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = defaultClosureRetryMaxAttempts
	}
	if p.InitialBackoff == 0 {
		p.InitialBackoff = defaultClosureRetryInitialBackoff
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = max(defaultClosureRetryMaxBackoff, p.InitialBackoff)
	}
	if p.MaxBackoff < p.InitialBackoff {
		return p, fmt.Errorf("closure retry max backoff %s is below the initial backoff %s", p.MaxBackoff, p.InitialBackoff)
	}
	return p, nil
}

func (p ClosureRetryPolicy) backoff(attempts int) time.Duration {
	delay := p.InitialBackoff
	for range attempts - 1 {
		if delay >= p.MaxBackoff/2 {
			return p.MaxBackoff
		}
		delay *= 2
	}
	return min(delay, p.MaxBackoff)
}

// ClosureFailure is the durable record of broker rejections of the pending
// closure. Permanent means the retry budget is spent and the closure is left
// for an operator; the pending closure keeps its evidence.
type ClosureFailure struct {
	IncidentID    string    `json:"incident_id"`
	Attempts      int       `json:"attempts"`
	MaxAttempts   int       `json:"max_attempts"`
	LastError     string    `json:"last_error"`
	FirstFailedAt time.Time `json:"first_failed_at"`
	LastFailedAt  time.Time `json:"last_failed_at"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	Permanent     bool      `json:"permanent"`
	Action        string    `json:"action,omitempty"`
}

func (f ClosureFailure) Error() string {
	return fmt.Sprintf(
		"incident %q closure permanently failed after %d broker attempts: %s; %s",
		f.IncidentID, f.Attempts, f.LastError, f.Action,
	)
}

func cloneClosureFailure(failure *ClosureFailure) *ClosureFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	return &cloned
}

func (c *Controller) clock() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

// closureRetryDueLocked reports whether a pending closure may be resubmitted.
// Callers hold c.mu.
func (c *Controller) closureRetryDueLocked() bool {
	failure := c.closureFailure
	if failure == nil {
		return true
	}
	return !failure.Permanent && !c.clock().Before(failure.NextAttemptAt)
}

// PausedWake is when a paused controller must wake: the next closure retry,
// or zero. A pause stops observing the application, not committing memory.
func (c *Controller) PausedWake() time.Time {
	return c.closureRetryWake(time.Time{})
}

// closureRetryWake returns the next closure retry time when it is earlier than
// next, so the runtime loop wakes to resubmit the closure.
func (c *Controller) closureRetryWake(next time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	failure := c.closureFailure
	if c.closureState != "pending" || failure == nil || failure.Permanent || failure.NextAttemptAt.IsZero() {
		return next
	}
	if next.IsZero() || failure.NextAttemptAt.Before(next) {
		return failure.NextAttemptAt
	}
	return next
}

// recordClosureRejectionLocked counts a failed closure attempt and either
// schedules the next attempt or marks the closure permanently failed. It
// returns the failure when this attempt spent the retry budget. Callers hold
// c.mu.
func (c *Controller) recordClosureRejectionLocked(err error) (ClosureFailure, bool) {
	c.closureState = "pending"
	if errors.Is(err, context.Canceled) {
		// Lost leadership or shutdown is not a broker verdict.
		return ClosureFailure{}, false
	}
	now := c.clock()
	failure := c.closureFailure
	incidentID := c.pendingClosure.Request.IncidentID
	if failure == nil || failure.IncidentID != incidentID {
		failure = &ClosureFailure{IncidentID: incidentID, FirstFailedAt: now}
	}
	policy := c.config.ClosureRetry
	failure.Attempts++
	failure.MaxAttempts = policy.MaxAttempts
	failure.LastError = err.Error()
	failure.LastFailedAt = now
	c.closureFailure = failure
	if failure.Attempts < policy.MaxAttempts {
		failure.NextAttemptAt = now.Add(policy.backoff(failure.Attempts))
		return ClosureFailure{}, false
	}
	failure.NextAttemptAt = time.Time{}
	failure.Permanent = true
	failure.Action = "the broker will not commit this closure as proposed; inspect the incident worktree and " +
		"the broker ledger, fix the proposal or the broker check, then set closure_state to \"pending\" and " +
		"remove closure_failure in the sdo-controller-state ConfigMap to retry; new incidents stay blocked until then"
	c.closureState = closureStateFailed
	return *failure, true
}

// ClosureFailure returns the durable closure rejection record, if any.
func (c *Controller) ClosureFailure() (ClosureFailure, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closureFailure == nil {
		return ClosureFailure{}, false
	}
	return *c.closureFailure, true
}

// ClosureFailureError returns the permanent closure failure as an error, or
// nil while the closure can still commit.
func ClosureFailureError(controller *Controller) error {
	failure, ok := controller.ClosureFailure()
	if !ok || !failure.Permanent {
		return nil
	}
	return failure
}

func validateClosureFailure(state RuntimeState) error {
	failure := state.ClosureFailure
	if state.ClosureState == closureStateFailed && (failure == nil || !failure.Permanent) {
		return fmt.Errorf("failed incident closure is missing its permanent failure record")
	}
	if failure == nil {
		return nil
	}
	if state.PendingClosure == nil || failure.IncidentID != state.PendingClosure.Request.IncidentID {
		return fmt.Errorf("closure failure for incident %q does not match the pending closure", failure.IncidentID)
	}
	if failure.Permanent != (state.ClosureState == closureStateFailed) {
		return fmt.Errorf("closure failure permanence does not match closure state %q", state.ClosureState)
	}
	return nil
}
