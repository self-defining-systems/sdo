package runtime

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// busyWindow anchors the published bound at the moment incident work began, so
// polling the state never moves it. It resets when the controller goes idle and
// is not persisted: a restarted controller anchors again at its first export.
type busyWindow struct {
	since time.Time
	bound time.Duration
}

// retryBackoffTotal is the delay a closure or queued reflection spends backing
// off before its retry budget is spent (2+4+8+16+32+60+60 s by default).
func (p ClosureRetryPolicy) retryBackoffTotal() time.Duration {
	var total time.Duration
	for attempt := 1; attempt < max(1, p.MaxAttempts); attempt++ {
		delay := p.InitialBackoff << min(attempt-1, 16)
		if p.MaxBackoff > 0 {
			delay = min(delay, p.MaxBackoff)
		}
		total += delay
	}
	return total
}

// incidentWorkOpenLocked reports whether the controller still owns incident
// work: an open incident, a closure in flight, or a reflection queued or
// running (an unreadable queue counts as one).
func (c *Controller) incidentWorkOpenLocked() (open bool, reflections int) {
	reflections = len(c.reflections.queued)
	if c.reflections.unknown {
		reflections++
	}
	incident := c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked()
	return incident || reflections > 0 || c.reflections.running != "", reflections
}

// busyUntilLocked is the one fact observers wait on: zero while idle, otherwise
// the latest time the controller can still be working. Each response and each
// reflection takes at most the response timeout plus its retry backoff; an
// incident in flight adds its verification and its own reflection.
func (c *Controller) busyUntilLocked(now time.Time) time.Time {
	open, reflections := c.incidentWorkOpenLocked()
	if !open {
		c.busy = busyWindow{}
		return time.Time{}
	}
	if c.reflections.running != "" && reflections == 0 {
		reflections = 1
	}
	perUnit := c.config.ResponseTimeout + c.config.ClosureRetry.retryBackoffTotal()
	units := reflections
	if c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked() {
		units += 2
	}
	bound := c.config.VerificationTimeout + time.Duration(units)*perUnit
	if c.busy.since.IsZero() {
		c.busy.since = now
	}
	c.busy.bound = max(c.busy.bound, bound)
	return c.busy.since.Add(c.busy.bound)
}

func optionalBusyUntil(until time.Time) *timestamppb.Timestamp {
	if until.IsZero() {
		return nil
	}
	return timestamppb.New(until.UTC())
}
