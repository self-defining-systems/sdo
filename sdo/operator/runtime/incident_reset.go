package runtime

import (
	"context"
	"fmt"
	"time"

	kclock "k8s.io/utils/clock"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// ResetIncidents clears the controller between problems so the next fault
// starts clean. An open incident (escalated or not) whose closure has not
// begun ends as a failed closure with reason RESET: its responder work is
// stopped, no outcome is recorded and nothing is learned. Pending batched
// openers and tracked finding states are dropped, so a fault that is still
// present opens a fresh incident after confirmation. A closure already begun
// completes normally. Memory and learned playbooks are untouched. It returns
// the reset incident's ID, empty when none was open.
func (c *Controller) ResetIncidents(ctx context.Context, now time.Time) (string, error) {
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	open := c.incidentOpen && request != nil && !c.closureBlocksNextLocked() &&
		(c.pendingClosure == nil || c.pendingClosure.Request.IncidentID != request.IncidentID)
	if !open {
		c.tracker.Restore(nil)
		c.batcher.Restore(BatcherState{})
		c.scheduler.ExpediteAll(now)
		c.mu.Unlock()
		return "", c.PersistState(ctx)
	}
	cancel := c.responderCancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ender, ok := c.dispatcher.(incidentWorkEnder); ok {
		endCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		err := ender.EndIncidentWork(endCtx, request.IncidentID)
		stop()
		if err != nil {
			return "", fmt.Errorf("end responder work of incident %s: %w", request.IncidentID, err)
		}
	}
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != request.IncidentID ||
		c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == request.IncidentID {
		// The incident closed or began its closure meanwhile.
		c.mu.Unlock()
		return "", nil
	}
	closure := IncidentClosure{
		Request: *request, Result: cloneIncidentResult(c.currentIncidentResult),
		ObservationGeneration: request.ObservationGeneration,
		DispatchError:         "reset by the operator between problems",
		DetectedAt:            c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: now, VerifiedAt: now, DetectorTimeline: cloneTimeline(c.timeline),
	}
	projected, err := incidentClosureContract(closure)
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected})
	}
	if err == nil && (c.pendingClosure == nil || c.pendingClosure.Request.IncidentID != request.IncidentID || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING) {
		err = fmt.Errorf("cannot reset incident %s while another closure is pending", request.IncidentID)
	}
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Reset, At: now, Error: closure.DispatchError})
	}
	if err != nil {
		c.mu.Unlock()
		return "", err
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseClosed, RecordedAt: contractTimestamp(now), IncidentId: request.IncidentID, DispatchError: true,
		AfterDispatchFindingKeys: afterDispatchKeys(c.timeline)})
	failure := cloneClosureFailure(c.closureFailure)
	c.clearIncidentLocked()
	c.responseStagesRunning = false
	c.timeline = nil
	c.tracker.Restore(nil)
	c.batcher.Restore(BatcherState{})
	c.scheduler.ExpediteAll(now)
	c.mu.Unlock()
	if err := c.PersistState(ctx); err != nil {
		return "", err
	}
	if failure != nil && c.OnClosureFailed != nil {
		c.OnClosureFailed(*failure)
	}
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
	return request.IncidentID, nil
}

// watchResets polls the reset ConfigMap and emits each requested generation
// once until it is acknowledged. Read errors are reported and polling
// continues.
func watchResets(ctx context.Context, resets install.ResetRequests, clock kclock.WithTicker, interval time.Duration, onError func(error)) <-chan string {
	if interval <= 0 {
		interval = time.Second
	}
	requests := make(chan string)
	go func() {
		defer close(requests)
		ticker := timerClock(clock).NewTicker(interval)
		defer ticker.Stop()
		emitted := ""
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C():
			}
			generation, err := resets.Pending(ctx)
			if err != nil {
				if ctx.Err() == nil && onError != nil {
					onError(err)
				}
				continue
			}
			if generation == "" || generation == emitted {
				continue
			}
			select {
			case requests <- generation:
				emitted = generation
			case <-ctx.Done():
				return
			}
		}
	}()
	return requests
}
