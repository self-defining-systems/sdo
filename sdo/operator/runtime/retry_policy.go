package runtime

import (
	"context"
	"errors"
	"time"

	"sdo.dev/operator/memory"
)

// ErrDispatchPermanent marks a failure that retrying cannot fix: the same
// inputs fail the same way (operational memory the reader rejects, a task over
// its budget, a request or contract that does not validate). classifyPermanent
// treats it, and the memory sentinels below, as permanent.
var ErrDispatchPermanent = errors.New("dispatch cannot be prepared")

// classifyPermanent reports whether err is a deterministic failure of its
// inputs rather than of the world around them. Everything else (context
// deadline or cancellation, Git or IO errors, Kubernetes API errors, and any
// error with no class) is transient: it retries under a bound, so an unclassed
// error costs time but never wedges.
func classifyPermanent(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, ErrDispatchPermanent) || errors.Is(err, memory.ErrUnreadable) ||
		errors.Is(err, memory.ErrSnapshotBudget) || errors.Is(err, memory.ErrSnapshotIncomplete)
}

// boundedRetry is a retry schedule for work that may fail transiently: the
// delay after the n-th consecutive failure of one subject is First doubled n-1
// times, capped at Max, and the subject gives up Bound after its first failure.
// The next attempt never falls past that bound, so the last attempt and the
// give-up come at the bound, not after it.
type boundedRetry struct {
	First, Max, Bound time.Duration
}

// retryLedger keeps each subject's failure streak for one boundedRetry. A
// subject is any stable key (an incident ID, an object reference). The ledger
// has no lock: callers serialize access (the controller holds c.mu).
type retryLedger struct {
	policy   boundedRetry
	subjects map[string]retryStreak
}

type retryStreak struct {
	firstFailure time.Time
	attempts     int
}

func newRetryLedger(policy boundedRetry) *retryLedger {
	return &retryLedger{policy: policy, subjects: map[string]retryStreak{}}
}

// failed records a failure of subject at now. It returns when the next attempt
// may run, or exhausted once the subject's bound has passed (the caller gives up
// and the streak is forgotten).
func (l *retryLedger) failed(subject string, now time.Time) (next time.Time, exhausted bool) {
	streak := l.subjects[subject]
	if streak.attempts == 0 {
		streak.firstFailure = now
	}
	streak.attempts++
	if now.Sub(streak.firstFailure) >= l.policy.Bound {
		delete(l.subjects, subject)
		return time.Time{}, true
	}
	l.subjects[subject] = streak
	delay := l.policy.First
	for range streak.attempts - 1 {
		if delay >= l.policy.Max/2 {
			delay = l.policy.Max
			break
		}
		delay *= 2
	}
	next = now.Add(min(delay, l.policy.Max))
	if bound := streak.firstFailure.Add(l.policy.Bound); next.After(bound) {
		next = bound
	}
	return next, false
}

// cleared forgets subject's streak: the work succeeded or no longer matters.
func (l *retryLedger) cleared(subject string) { delete(l.subjects, subject) }

// attempts is how many consecutive failures subject has recorded.
func (l *retryLedger) attempts(subject string) int { return l.subjects[subject].attempts }
