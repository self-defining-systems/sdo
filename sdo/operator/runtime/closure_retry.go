package runtime

import (
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"time"
)

const (
	defaultClosureRetryMaxAttempts    = 8
	defaultClosureRetryInitialBackoff = 2 * time.Second
	defaultClosureRetryMaxBackoff     = time.Minute
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

// ClosureFailure is the durable record of broker rejections of the pending
// closure. Permanent means the retry budget is spent and the closure is left
// for an operator; the pending closure keeps its evidence.
type ClosureFailure struct {
	IncidentID    string
	Attempts      int
	MaxAttempts   int
	LastError     string
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	NextAttemptAt time.Time
	Permanent     bool
	Action        string
	Reason        contracts.ClosureFailureReason
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

// PausedWake includes report grace and worker termination retries as well as
// closure retries. A pause stops observation, not already-started closure work.
func (c *Controller) PausedWake() time.Time {
	c.mu.Lock()
	next := c.earlyReportWakeLocked()
	if retry := c.pausedEffectRetryAt; !retry.IsZero() && (next.IsZero() || retry.Before(next)) {
		next = retry
	}
	c.mu.Unlock()
	return c.closureRetryWake(next)
}

// closureRetryWake returns the next closure retry time when it is earlier than
// next, so the runtime loop wakes to resubmit the closure.
func (c *Controller) closureRetryWake(next time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	failure := c.closureFailure
	if c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING || failure == nil || failure.Permanent || failure.NextAttemptAt.IsZero() {
		return next
	}
	if next.IsZero() || failure.NextAttemptAt.Before(next) {
		return failure.NextAttemptAt
	}
	return next
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
