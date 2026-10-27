package runtime

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// busyWindow remembers the bound last published, so polling the state never
// moves it, and re-anchors it whenever a new unit of work begins (the unit
// count rises or a different incident or reflection is the one in hand). It
// resets when the controller goes idle and is not persisted: a restarted
// controller anchors again at its first export.
type busyWindow struct {
	until time.Time
	units int
	key   string
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
// work: an open incident, a closure in flight, a reflection queued or running
// (an unreadable queue counts as one), or a finding that will become an incident
// (pending in the batcher or not yet dispatched, as quiet() sees it).
func (c *Controller) incidentWorkOpenLocked() (inFlight bool, reflections int) {
	reflections = len(c.reflections.queued)
	if c.reflections.unknown {
		reflections++
	}
	if c.reflections.running != "" && reflections == 0 {
		reflections = 1
	}
	_, batched := c.batcher.Deadline()
	inFlight = c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked() ||
		batched || !c.tracker.Quiet()
	return inFlight, reflections
}

// busyKeyLocked names the unit of work in hand, so a different one re-anchors the bound.
func (c *Controller) busyKeyLocked() string {
	key := c.reflections.running
	if len(c.reflections.queued) > 0 {
		key += "|" + c.reflections.queued[0]
	}
	if c.currentIncidentRequest != nil {
		key += "|" + c.currentIncidentRequest.IncidentID
	}
	return key
}

// busyUntilLocked is the one fact observers wait on: zero exactly while the
// controller owns no incident work, otherwise a generous backstop on when it can
// still be working. Observers wait for idle; the bound only ends a controller
// that never gets there. A queued reflection is retried up to MaxAttempts times
// and then abandoned in one more pass, each at most the response timeout, with
// the retry backoff between; an incident in flight adds its response, closure
// retry and verification plus its own reflection.
func (c *Controller) busyUntilLocked(now time.Time) time.Time {
	inFlight, reflections := c.incidentWorkOpenLocked()
	if !inFlight && reflections == 0 {
		c.busy = busyWindow{}
		return time.Time{}
	}
	backoff := c.config.ClosureRetry.retryBackoffTotal()
	response := c.config.ResponseTimeout + backoff
	reflection := time.Duration(max(1, c.config.ClosureRetry.MaxAttempts)+1)*c.config.ResponseTimeout + backoff
	units := reflections
	bound := time.Duration(reflections) * reflection
	if inFlight {
		units++
		bound += c.config.VerificationTimeout + response + reflection
	}
	key := c.busyKeyLocked()
	if c.busy.until.IsZero() || units > c.busy.units || key != c.busy.key {
		if until := now.Add(bound); until.After(c.busy.until) {
			c.busy.until = until
		}
	}
	c.busy.units, c.busy.key = units, key
	return c.busy.until
}

func optionalBusyUntil(until time.Time) *timestamppb.Timestamp {
	if until.IsZero() {
		return nil
	}
	return timestamppb.New(until.UTC())
}
