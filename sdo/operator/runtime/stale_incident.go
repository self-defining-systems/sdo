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

// retireExpiredIncident refuses to resume an abandoned request after its
// response deadline. Terminal evidence goes through the existing typed failure
// archive, so a deleted proposal cannot block the next incident's dispatch.
func (c *Controller) retireExpiredIncident(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	eligible := c.canRetireExpiredIncidentLocked(now)
	c.mu.Unlock()
	if !eligible {
		return nil
	}
	if checker, ok := c.dispatcher.(interface {
		IncidentWorkActive(context.Context, string) (bool, error)
	}); ok {
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
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != request.IncidentID || !c.canRetireExpiredIncidentLocked(now) {
		c.mu.Unlock()
		return nil
	}
	closure := IncidentClosure{
		Request: *request, Result: cloneIncidentResult(c.currentIncidentResult),
		ObservationGeneration: request.ObservationGeneration,
		DispatchError:         "response deadline expired with no live incident work",
		DetectedAt:            c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: now, VerifiedAt: now, DetectorTimeline: cloneTimeline(c.timeline),
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
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.ResponseExpired, At: now, Error: closure.DispatchError})
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

// canRetireExpiredIncidentLocked leaves claimed closures with their owner,
// including terminal failures whose original reason and notification must survive.
// A failed closure from an earlier incident does not claim the current incident.
func (c *Controller) canRetireExpiredIncidentLocked(now time.Time) bool {
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || request.ResponseDeadline.IsZero() || now.Before(request.ResponseDeadline) || c.responderDone ||
		c.dispatchState == contracts.DispatchState_DISPATCH_STATE_RUNNING || c.dispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING {
		return false
	}
	if c.closureBlocksNextLocked() || c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == request.IncidentID ||
		c.closureFailure != nil && c.closureFailure.IncidentID == request.IncidentID {
		return false
	}
	return true
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
