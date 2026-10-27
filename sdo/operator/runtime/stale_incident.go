package runtime

import (
	"context"
	"fmt"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// IncidentWork is the durable evidence of who owns open incident work.
type IncidentWork struct {
	// Active reports a live, pending, or terminating responder worker.
	Active bool
	// FinishedAt is when the newest terminal responder Job (the current
	// attempt) finished, or zero when none survives.
	FinishedAt time.Time
}

// IncidentWork reads durable work before an expired request is retired. API
// uncertainty is an error, never evidence that a worker is gone.
func (d KubernetesJobDispatcher) IncidentWork(ctx context.Context, incidentID string) (IncidentWork, error) {
	inventory, err := d.responderExecutions(ctx, incidentID, 0)
	if err != nil {
		return IncidentWork{}, err
	}
	return IncidentWork{Active: inventory.active(), FinishedAt: inventory.finishedAt()}, nil
}

// incidentWorkChecker proves whether open incident work still has an owner.
type incidentWorkChecker interface {
	IncidentWork(ctx context.Context, incidentID string) (IncidentWork, error)
}

// finishedHoldUntil bounds how long a finished responder Job whose result is
// unconsumed holds its incident: re-dispatch adopts that Job and harvests its
// result within the verification bound, or the result is lost.
func (c *Controller) finishedHoldUntil(work IncidentWork) time.Time {
	if work.FinishedAt.IsZero() {
		return time.Time{}
	}
	return work.FinishedAt.Add(c.config.VerificationTimeout + ownerLostGrace)
}

// ownerLostGrace bounds how long open incident work may outlive its owner past
// the durable response or verification bound before it fails as OWNER_LOST.
const ownerLostGrace = time.Minute

// retirement is a terminal path for an open incident with no live work.
type retirement struct {
	kind   lifecyclecore.EventKind
	reason string
}

// retireExpiredIncident refuses to resume an abandoned request after its
// response deadline, and fails work that lost its owner (the responder Job or
// the controller that would begin its closure). Every bound derives from durable
// state, so a restarted or paused controller retires it the same way. Terminal
// evidence goes through the existing typed failure archive, so a deleted
// proposal cannot block the next incident's dispatch.
func (c *Controller) retireExpiredIncident(ctx context.Context, now time.Time) error {
	checker, checked := c.dispatcher.(incidentWorkChecker)
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	resultConsumed := c.responderDone
	retire, eligible := c.incidentRetirementLocked(now, checked)
	c.mu.Unlock()
	if !eligible {
		return nil
	}
	reason := retire.reason
	if checked {
		work, err := checker.IncidentWork(ctx, request.IncidentID)
		if err != nil {
			return err
		}
		if work.Active {
			return nil
		}
		if until := c.finishedHoldUntil(work); !resultConsumed && !until.IsZero() {
			if now.Before(until) {
				c.mu.Lock()
				c.finishedHoldWake = until
				c.mu.Unlock()
				return nil
			}
			reason = "responder Job finished but its result was not harvested within the verification bound"
		}
	}
	usage, err := c.responderUsage(request.IncidentID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if current, ok := c.incidentRetirementLocked(now, checked); c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != request.IncidentID || !ok || current != retire {
		c.mu.Unlock()
		return nil
	}
	completedAt := now
	if c.responderDone && !c.responderCompletedAt.IsZero() {
		completedAt = c.responderCompletedAt
	}
	closure := IncidentClosure{
		Request: *request, Result: cloneIncidentResult(c.currentIncidentResult),
		ObservationGeneration: request.ObservationGeneration,
		DispatchError:         reason,
		DetectedAt:            c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: completedAt, VerifiedAt: now, DetectorTimeline: cloneTimeline(c.timeline),
	}
	if usage != nil {
		if closure.Result == nil {
			closure.Result = &IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentFailed, Timing: TimingMetrics{StartedAt: now, CompletedAt: now}}
		}
		closure.Result.Usage = *usage
	}
	projected, err := incidentClosureContract(closure)
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected})
	}
	if err == nil && (c.pendingClosure == nil || c.pendingClosure.Request.IncidentID != request.IncidentID || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING) {
		err = fmt.Errorf("cannot retire expired incident while another closure is pending")
	}
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: retire.kind, At: now, Error: closure.DispatchError})
	}
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseClosed, RecordedAt: contractTimestamp(now), IncidentId: request.IncidentID, DispatchError: true})
	failure := cloneClosureFailure(c.closureFailure)
	c.clearIncidentLocked()
	c.timeline = nil
	// Fresh observations of a still-firing detector must activate a new incident.
	c.tracker.Restore(nil)
	c.batcher.Restore(BatcherState{})
	c.scheduler.ExpediteAll(now)
	c.mu.Unlock()
	if err := c.PersistState(ctx); err != nil {
		return err
	}
	if failure != nil && c.OnClosureFailed != nil {
		c.OnClosureFailed(*failure)
	}
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
	return nil
}

// incidentRetirementLocked leaves claimed closures with their owner, including
// terminal failures whose original reason and notification must survive. A
// failed closure from an earlier incident does not claim the current incident.
// Open work past its bound retires only after the caller proves that no work
// holds it, live or finished but unharvested: a running dispatch needs that
// proof from the API (workActiveCheckable).
func (c *Controller) incidentRetirementLocked(now time.Time, workActiveCheckable bool) (retirement, bool) {
	retire, due, ok := c.incidentRetirementDueLocked(workActiveCheckable)
	return retire, ok && !now.Before(due)
}

// incidentRetirementDueLocked returns the durable bound after which open
// incident work retires, and the terminal path it takes.
func (c *Controller) incidentRetirementDueLocked(workActiveCheckable bool) (retirement, time.Time, bool) {
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || request.ResponseDeadline.IsZero() {
		return retirement{}, time.Time{}, false
	}
	if c.closureBlocksNextLocked() || c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == request.IncidentID ||
		c.closureFailure != nil && c.closureFailure.IncidentID == request.IncidentID {
		return retirement{}, time.Time{}, false
	}
	switch {
	case c.responderDone:
		// Verification closes within VerificationTimeout while a controller
		// observes; an incident still open past it has lost that owner.
		if c.responderCompletedAt.IsZero() {
			return retirement{}, time.Time{}, false
		}
		return retirement{lifecyclecore.OwnerLost, "responder finished but no closure began within the verification bound; no live incident work"},
			c.responderCompletedAt.Add(c.config.VerificationTimeout + ownerLostGrace), true
	case c.dispatchState == contracts.DispatchState_DISPATCH_STATE_RUNNING || c.dispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING:
		if !workActiveCheckable {
			return retirement{}, time.Time{}, false
		}
		return retirement{lifecyclecore.OwnerLost, "dispatch still running past the response deadline with no live incident work"},
			request.ResponseDeadline.Add(ownerLostGrace), true
	}
	return retirement{lifecyclecore.ResponseExpired, "response deadline expired with no live incident work"}, request.ResponseDeadline, true
}

// retirementWakeLocked is the next future retirement bound. A past bound held
// by live work waits for that work's own completion instead of spinning; one
// held by a finished Job wakes when that hold ends.
func (c *Controller) retirementWakeLocked(now time.Time) time.Time {
	_, checkable := c.dispatcher.(incidentWorkChecker)
	_, due, ok := c.incidentRetirementDueLocked(checkable)
	switch {
	case !ok:
		return time.Time{}
	case due.After(now):
		return due
	case c.finishedHoldWake.After(now):
		return c.finishedHoldWake
	}
	return time.Time{}
}

func (c *Controller) clearIncidentLocked() {
	c.closureStableSince = time.Time{}
	c.closureMutationRevision = ""
	c.earlyLastMutationAt = time.Time{}
	c.responderLoop = nil
	c.midStage = midIncidentStage{}
	c.responderCancel = nil
	c.finishedHoldWake = time.Time{}
	c.earlyStopRetryAt = time.Time{}
	c.incidentOpen = false
	c.responderDone = false
	c.currentIncidentRequest = nil
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.incidentDetectedAt = time.Time{}
	c.incidentDispatchedAt = time.Time{}
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_IDLE
	c.incidentFindingKeys = nil
	c.cleanedHelpers = nil
	c.resetDispatchRetryLocked()
}
