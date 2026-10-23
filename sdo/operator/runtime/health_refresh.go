package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// acceptedHealthGuard binds every evaluation/effect to the policy compiled into
// this process. Memory-only commits are safe; a policy or source advance asks
// the parent builder to refresh and adopt a newly compiled detector bundle.
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

// An investigation started against older application inputs cannot authorize
// writes or successful closure after adoption; it needs fresh investigation.
var ErrStaleHealthInvestigation = errors.New("health unverified: incident predates accepted source or objective; fresh investigation required")

func checkInvestigationHealth(ctx context.Context, root, incidentRevision, compiledRevision string) error {
	if incidentRevision == "" {
		return nil
	}
	incident, err := memory.HealthInputs(ctx, root, incidentRevision)
	if err != nil {
		return err
	}
	current, err := memory.HealthInputs(ctx, root, compiledRevision)
	if err != nil {
		return err
	}
	if incident.SourceInputs != current.SourceInputs || incident.Objective != current.Objective || incident.DetectorRevision != current.DetectorRevision {
		return ErrStaleHealthInvestigation
	}
	return nil
}

// healthGuardedPlanClient checks again at the write boundary. A source advance
// during plan preparation must not turn a previously authorized plan into a write.
type healthGuardedPlanClient struct {
	PlanObjectClient
	Check func(context.Context) error
}

func (c healthGuardedPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return err
		}
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, patch)
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
