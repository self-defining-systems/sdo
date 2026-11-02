package runtime

import "time"

const recoveryStabilitySummary = "awaiting controller recovery stability proof"

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
	deadline := time.Time{}
	if c.currentIncidentRequest != nil {
		deadline = c.currentIncidentRequest.ResponseDeadline
		if c.responderDone {
			deadline = c.verificationDeadlineLocked()
		}
	}
	return c.recoveryProofForEpochLocked(now, states, safe, c.recoveryObservationEpochLocked(), deadline)
}

func (c *Controller) recoveryProofForEpochLocked(now time.Time, states []DetectorEvaluation, safe bool, since, deadline time.Time) bool {
	if !safe {
		c.closureStableSince = time.Time{}
		c.recoverySafetyLost = true
		return false
	}
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
	stableAt := c.closureStableSince.Add(c.config.ClosureStabilityWindow)
	return !now.Before(stableAt) && (deadline.IsZero() || !stableAt.After(deadline))
}

// Completed receipt timestamps bound the applied write, independently of how
// long the automatic stage or final narration later takes. An unjournalled
// mutation uses its first controller observation instead.
func lastRepairCompletion(result *IncidentResult) (time.Time, bool) {
	var latest time.Time
	if result != nil {
		for _, receipt := range result.RepairActions {
			if receipt.CompletedAt.After(latest) {
				latest = receipt.CompletedAt
			}
		}
	}
	return latest, !latest.IsZero()
}
