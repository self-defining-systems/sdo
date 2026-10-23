package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/lifecyclecore"
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

// controllerHealthGuard applies the same evidence binding to live investigations,
// frozen closures, and the retained closure awaiting acknowledgment.
func controllerHealthGuard(c *Controller, root, compiled string) func(context.Context) error {
	policy := acceptedHealthGuard(root, compiled)
	c.checkInvestigationHealth = func(ctx context.Context, revision string) error {
		return checkInvestigationHealth(ctx, root, revision, compiled)
	}
	return func(ctx context.Context) error {
		if err := policy(ctx); err != nil {
			return err
		}
		c.mu.Lock()
		request := cloneIncidentRequest(c.currentIncidentRequest)
		pending := cloneIncidentClosure(c.pendingClosure)
		phase := c.closureState
		receipt := cloneClosureReceipt(c.closureReceipt)
		broker := c.broker
		c.mu.Unlock()
		committed := phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED && receipt != nil
		replay := receipt == nil && (phase == contracts.ClosureState_CLOSURE_STATE_PENDING || phase == contracts.ClosureState_CLOSURE_STATE_PROCESSING)
		if pending != nil && (committed || replay) && (request == nil || request.IncidentID == pending.Request.IncidentID) {
			if err := checkInvestigationHealth(ctx, root, pending.Request.SourceCommit, compiled); errors.Is(err, ErrStaleHealthInvestigation) {
				return checkCommittedRepairHealth(ctx, root, compiled, *pending, receipt, broker)
			} else if err != nil {
				return err
			}
		}
		if request != nil {
			if err := checkInvestigationHealth(ctx, root, request.SourceCommit, compiled); err != nil {
				return err
			}
		}
		if pending != nil && phase != contracts.ClosureState_CLOSURE_STATE_FAILED {
			return checkInvestigationHealth(ctx, root, pending.Request.SourceCommit, compiled)
		}
		if receipt != nil && pending == nil {
			return ErrStaleHealthInvestigation
		}
		return nil
	}
}

// Only complete native broker evidence can authorize finishing an already committed
// incident across adoption of that incident's own accepted source repair.
// A restart may precede persistence of the controller receipt, so a frozen
// closure may replay the idempotent broker effect using its existing receipt.
// This never authorizes an investigation or new publication using stale policy.
type committedRepairHealthBroker interface {
	committedRepairHealthRevision(context.Context, IncidentClosure, *ClosureReceipt) (string, error)
}

func checkCommittedRepairHealth(ctx context.Context, root, compiled string, closure IncidentClosure, receipt *ClosureReceipt, broker IncidentBroker) error {
	proof, ok := broker.(committedRepairHealthBroker)
	if !ok {
		return ErrStaleHealthInvestigation
	}
	proposal, err := proof.committedRepairHealthRevision(ctx, closure, receipt)
	if err != nil {
		return err
	}
	old, err := memory.HealthInputs(ctx, root, closure.Request.SourceCommit)
	if err != nil {
		return err
	}
	current, err := memory.HealthInputs(ctx, root, compiled)
	if err != nil {
		return err
	}
	repair, err := memory.HealthInputs(ctx, root, proposal)
	if err != nil {
		return err
	}
	if old.Objective != current.Objective || old.DetectorRevision != current.DetectorRevision || repair.SourceInputs != current.SourceInputs || repair.Objective != current.Objective {
		return ErrStaleHealthInvestigation
	}
	return nil
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

// prepareHealthEffects retires obsolete investigations at every driver entry,
// including startup and maintenance pauses. It never evaluates application
// detectors, and retirement must be durable before any effect is authorized.
func (c *Controller) prepareHealthEffects(ctx context.Context, now time.Time) error {
	if c.CheckHealth == nil {
		return nil
	}
	if err := c.CheckHealth(ctx); err != nil {
		if !errors.Is(err, ErrStaleHealthInvestigation) {
			return err
		}
		if err := c.supersedeStaleHealth(ctx, now); err != nil {
			return err
		}
		return c.CheckHealth(ctx)
	}
	return nil
}

// supersedeStaleHealth terminally archives obsolete evidence without submitting
// a repair or a successful outcome. The archive outbox is persisted before new
// incident effects can run. Late responder/broker completions no longer match.
func (c *Controller) supersedeStaleHealth(ctx context.Context, now time.Time) error {
	if c.checkInvestigationHealth == nil {
		return ErrStaleHealthInvestigation
	}
	c.mu.Lock()
	retire := func(closure *IncidentClosure) error {
		projected, err := incidentClosureContract(*closure)
		if err != nil {
			return err
		}
		if _, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected}); err != nil {
			return err
		}
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Superseded, At: now, Error: ErrStaleHealthInvestigation.Error()})
		return err
	}
	var err error
	if c.pendingClosure != nil && c.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED {
		err = c.checkInvestigationHealth(ctx, c.pendingClosure.Request.SourceCommit)
		if errors.Is(err, ErrStaleHealthInvestigation) {
			_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Superseded, At: now, Error: ErrStaleHealthInvestigation.Error()})
		}
	}
	if err == nil && c.currentIncidentRequest != nil {
		err = c.checkInvestigationHealth(ctx, c.currentIncidentRequest.SourceCommit)
		if errors.Is(err, ErrStaleHealthInvestigation) {
			detected := c.incidentDetectedAt
			if detected.IsZero() {
				detected = now
			}
			dispatched := c.incidentDispatchedAt
			if dispatched.IsZero() {
				dispatched = now
			}
			closure := &IncidentClosure{Request: *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult), DispatchError: ErrStaleHealthInvestigation.Error(), DetectedAt: detected, DispatchedAt: dispatched, ResponderCompletedAt: now, VerifiedAt: now}
			err = retire(closure)
			if err == nil {
				c.incidentOpen, c.responderDone = false, false
				c.currentIncidentRequest, c.currentIncidentResult = nil, nil
				c.dispatchState = contracts.DispatchState_DISPATCH_STATE_IDLE
				c.dispatchError = ""
				c.incidentDetectedAt, c.incidentDispatchedAt, c.responderCompletedAt = time.Time{}, time.Time{}, time.Time{}
				c.incidentFindingKeys, c.cleanedHelpers = nil, nil
			}
		}
	}
	failure := cloneClosureFailure(c.closureFailure)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err := c.PersistState(ctx); err != nil {
		return err
	}
	if failure != nil {
		c.reportBrokerError("supersede stale health investigation", *failure)
		if c.OnClosureFailed != nil {
			c.OnClosureFailed(*failure)
		}
	}
	return nil
}
