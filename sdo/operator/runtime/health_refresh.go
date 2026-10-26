package runtime

import (
	"context"
	"sync"
	"time"

	"sdo.dev/operator/memory"
)

// acceptedHealthGuard binds every evaluation/effect to the policy compiled into
// this process. Memory-only commits are safe; a policy or source advance asks
// the parent builder to refresh and adopt a newly compiled detector bundle and
// restart. It never retires an incident or closure: the restarted controller
// resumes them, and health verification alone decides closure.
func acceptedHealthGuard(root, compiledRevision string) func(context.Context) error {
	var mu sync.Mutex
	var built *memory.HealthRevision
	return func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if built == nil {
			revision, err := memory.HealthInputs(ctx, root, compiledRevision)
			if err != nil {
				return err
			}
			built = &revision
		}
		accepted, err := memory.AcceptedHealthRevision(ctx, root)
		if err != nil {
			return err
		}
		if accepted.NeedsRefresh || accepted.SourceInputs != built.SourceInputs || accepted.Objective != built.Objective || accepted.DetectorRevision != built.DetectorRevision {
			return memory.ErrHealthRefreshRequired
		}
		return nil
	}
}

// discardRestoredHealthObservations prevents a queued finding from the previous
// binary becoming a new investigation before the adopted health policy runs.
// Active investigations retain their evidence and are separately revision-bound.
func (c *Controller) discardRestoredHealthObservations(now time.Time) {
	c.mu.Lock()
	health := make(map[string]bool, len(c.healthDetectorIDs))
	for _, id := range c.healthDetectorIDs {
		health[id] = true
		delete(c.detectorClearSince, id)
		c.scheduler.Expedite(id, now)
	}
	states := c.tracker.Snapshot()
	for key, state := range states {
		if health[state.DetectorID] {
			delete(states, key)
		}
	}
	c.tracker.Restore(states)
	batch := c.batcher.Snapshot()
	kept := batch.Findings[:0]
	for _, finding := range batch.Findings {
		if !health[finding.DetectorID] {
			kept = append(kept, finding)
		}
	}
	batch.Findings = kept
	c.batcher.Restore(batch)
	history := c.history[:0]
	for _, evaluation := range c.history {
		if !health[evaluation.DetectorID] {
			history = append(history, evaluation)
		}
	}
	c.history = history
	c.mu.Unlock()
	c.forgetGate()
}

// requireAdoptedHealth stops a driver pass while the accepted health policy
// needs a detector rebuild, at every entry including startup and maintenance
// pauses. It never evaluates application detectors and never retires work:
// the rebuilt controller resumes every open incident and closure.
func (c *Controller) requireAdoptedHealth(ctx context.Context) error {
	if c.CheckHealth == nil {
		return nil
	}
	return c.CheckHealth(ctx)
}
