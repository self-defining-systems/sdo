package runtime

import (
	"context"
	"fmt"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// IncidentWorkActive checks durable work before an expired request is retired.
// API uncertainty is an error, never evidence that an existing worker is gone.
func (d KubernetesJobDispatcher) IncidentWorkActive(ctx context.Context, incidentID string) (bool, error) {
	inventory, err := d.responderExecutions(ctx, incidentID, 0)
	if err != nil {
		return false, err
	}
	return inventory.active(), nil
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
	checker, checked := c.dispatcher.(interface {
		IncidentWorkActive(context.Context, string) (bool, error)
	})
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	retire, eligible := c.incidentRetirementLocked(now, checked)
	c.mu.Unlock()
	if !eligible {
		return nil
	}
	if checked {
		active, err := checker.IncidentWorkActive(ctx, request.IncidentID)
		if err != nil {
			return err
		}
		if active {
			return nil
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
		DispatchError:         retire.reason,
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
// Open work past its bound retires only after the caller proves no live work:
// a running dispatch needs that proof from the API (workActiveCheckable).
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
// by live work waits for that work's own completion instead of spinning.
func (c *Controller) retirementWakeLocked(now time.Time) time.Time {
	_, checkable := c.dispatcher.(interface {
		IncidentWorkActive(context.Context, string) (bool, error)
	})
	if _, due, ok := c.incidentRetirementDueLocked(checkable); ok && due.After(now) {
		return due
	}
	return time.Time{}
}

func (c *Controller) clearIncidentLocked() {
	c.closureStableSince = time.Time{}
	c.closureMutationRevision = ""
	c.earlyLastMutationAt = time.Time{}
	c.responderLoop = nil
	c.responderCancel = nil
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
