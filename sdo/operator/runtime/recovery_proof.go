package runtime

import "time"

// Narration is not a mutation. Completed responder turns retain the same
// observation epoch as their writes, so completion cannot start a second clock.
// A response without a registered mutation observer retains the legacy fresh
// post-response requirement.
func (c *Controller) recoveryObservationEpochLocked() time.Time {
	if !c.earlyLastMutationAt.IsZero() {
		return c.earlyLastMutationAt
	}
	if c.responderLoop == nil {
		return c.responderCompletedAt
	}
	return c.incidentDispatchedAt
}

// recoveryProofLocked is the sole stability decision. Clearing persistence
// and recovery stability consume the same continuous detector observations.
// Call only after fresh health and every write/rollout prerequisite holds.
// A restart never reuses an observer's in-memory proof.
func (c *Controller) recoveryProofLocked(now time.Time, states []DetectorEvaluation, safe bool) bool {
	if !safe || len(states) == 0 {
		c.closureStableSince = time.Time{}
		c.recoverySafetyLost = true
		return false
	}
	since := c.recoveryObservationEpochLocked()
	if c.recoveryObserverStarted.After(since) {
		since = c.recoveryObserverStarted
	}
	for _, state := range states {
		clear, ok := c.detectorClearSince[state.DetectorID]
		if !ok || state.Status != DetectorEvaluationClear {
			c.closureStableSince = time.Time{}
			return false
		}
		if clear.After(since) {
			since = clear
		}
	}
	if c.recoverySafetyLost {
		since = now
		c.recoverySafetyLost = false
	}
	// Starting a new observer requires a fresh window, even if persisted detector
	// streaks predate its restart. A live observer may overlap tracker clearing.
	if c.closureStableSince.IsZero() {
		c.closureStableSince = since
	}
	if since.After(c.closureStableSince) {
		c.closureStableSince = since
	}
	return !now.Before(c.closureStableSince.Add(c.config.ClosureStabilityWindow))
}
