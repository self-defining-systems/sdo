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
	return c.recoveryProofForEpochLocked(now, states, safe, c.recoveryObservationEpochLocked())
}

// recoveryWindowRunningLocked reports a stability window that is still running
// over clear health: the proof is only waiting for time to pass. The
// verification timeout bounds findings and unconfirmed observation, never this.
// A window that restarts at every pass (safety lost again, a responder still
// writing) is not running; it never completes, so the timeout still applies.
func (c *Controller) recoveryWindowRunningLocked(now time.Time) bool {
	return !c.closureStableSince.IsZero() && c.closureStableSince.Before(now) && len(c.activeHealthDetectors()) == 0
}

func (c *Controller) recoveryProofForEpochLocked(now time.Time, states []DetectorEvaluation, safe bool, since time.Time) bool {
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
	return !now.Before(stableAt)
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
