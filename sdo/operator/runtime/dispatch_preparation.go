package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Preparing the responder's task (reading operational memory, freezing the
// request) is a step of the controller's dispatch, never a precondition of
// saving state: no durable save may depend on an optional side computation. A
// failure is classified. A permanent one (unreadable memory, a task over
// budget) ends this dispatch at once; a transient one (Git, IO or Kubernetes
// API errors, a deadline) retries with bounded exponential backoff and ends it
// the same way when response.dispatch_prepare_bound passes. The incident stays
// open: a page closes at its bound, an alert-opened incident starts a fresh,
// paced session (restartSessionLocked) while its alerts fail.

const (
	dispatchPrepareFirstBackoff = 2 * time.Second
	dispatchPrepareMaxBackoff   = 60 * time.Second
	// defaultDispatchPrepareBound is response.dispatch_prepare_bound's default.
	defaultDispatchPrepareBound = 2 * time.Minute
)

// dispatchPreparer is a dispatcher that freezes the worker task before the
// controller records its pending dispatch.
type dispatchPreparer interface {
	PrepareDispatch(context.Context, IncidentRequest) (IncidentRequest, error)
}

// DispatchPrepareError is a failure to prepare the responder's task, as opposed
// to a failure of the responder's run. The controller applies the preparation
// policy to it wherever it surfaces.
type DispatchPrepareError struct{ Err error }

func (e *DispatchPrepareError) Error() string { return e.Err.Error() }
func (e *DispatchPrepareError) Unwrap() error { return e.Err }

// dispatchPrepareRetry is the retry schedule of dispatch preparation: bounded
// by response.dispatch_prepare_bound, and never longer than the response timeout.
func dispatchPrepareRetry(config ControllerConfig) boundedRetry {
	bound := config.DispatchPrepareBound
	if bound <= 0 {
		bound = defaultDispatchPrepareBound
	}
	if config.ResponseTimeout > 0 {
		bound = min(bound, config.ResponseTimeout)
	}
	return boundedRetry{First: dispatchPrepareFirstBackoff, Max: dispatchPrepareMaxBackoff, Bound: bound}
}

// dispatchNeedsPreparationLocked reports a pending dispatch whose worker task is
// not frozen yet. Callers hold c.mu. An automatic stage that runs first freezes
// its own hand-off later, so it is not waiting on this step.
func (c *Controller) dispatchNeedsPreparationLocked() bool {
	if _, ok := c.dispatcher.(dispatchPreparer); !ok {
		return false
	}
	request := c.currentIncidentRequest
	return c.dispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING && request != nil && request.PreparedAgentTask == "" &&
		!c.automaticResponsePending(request)
}

// PrepareDispatchStep freezes the pending dispatch's worker task, or applies
// the preparation policy to its failure. It reports whether the controller's
// state changed, so the caller saves it. It returns an error only when it could
// not act at all (a lost lease, a cancelled context); a preparation failure is
// a recorded outcome, never an error that blocks a state save.
func (c *Controller) PrepareDispatchStep(ctx context.Context) (bool, error) {
	c.mu.Lock()
	if !c.dispatchNeedsPreparationLocked() || !c.dispatchRetryDueLocked() {
		c.mu.Unlock()
		return false, nil
	}
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	c.mu.Unlock()
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	preparer, ok := c.dispatcher.(dispatchPreparer)
	if !ok {
		return false, nil
	}
	prepared, err := preparer.PrepareDispatch(actionCtx, stageDispatchRequest(request))
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return c.noteDispatchPrepareFailure(request.IncidentID, err), nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentIncidentRequest == nil || !sameIncidentRequest(*c.currentIncidentRequest, request) {
		// The incident changed while the task was read; the next pass prepares
		// the current one.
		return false, nil
	}
	if deterministicStageReport(request.PriorStages).GetHandoffSelected() {
		c.currentIncidentRequest.PreparedAgentTask = prepared.PreparedAgentTask
		c.currentIncidentRequest.MaxResponseLoops = prepared.MaxResponseLoops
	} else {
		c.currentIncidentRequest = cloneIncidentRequest(&prepared)
	}
	c.publishHeldLocked()
	c.prepareRetries.cleared(request.IncidentID)
	c.resetDispatchRetryLocked()
	return true, nil
}

// noteDispatchPrepareFailure applies the preparation policy to err for the open
// incident incidentID and reports whether the controller's state changed (an
// ended dispatch or a scheduled retry that a save should record).
func (c *Controller) noteDispatchPrepareFailure(incidentID string, err error) bool {
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != incidentID {
		c.mu.Unlock()
		return false
	}
	now := c.clock()
	class := "permanent"
	if !classifyPermanent(err) {
		next, exhausted := c.prepareRetries.failed(incidentID, now)
		if !exhausted {
			c.dispatchNextRetryAt = next
			attempts := c.prepareRetries.attempts(incidentID)
			c.mu.Unlock()
			slog.Warn("responder_dispatch_preparation_retry", "incident_id", incidentID, "attempt", attempts, "retry_at", next, "error", err)
			return false
		}
		class = "transient_bound_exceeded"
	}
	c.prepareRetries.cleared(incidentID)
	c.endDispatchFailureLocked(now, class, err)
	c.mu.Unlock()
	if c.OnError != nil {
		c.OnError(fmt.Errorf("dispatch incident: %w", err))
	}
	return true
}

// endDispatchFailureLocked ends the incident's response without a
// responder: it records the failure and emits responder_dispatch_failed. The
// incident stays open: Recovered closes it, a page fails at its bound, and an
// alert-opened incident starts a fresh, paced session. Callers hold c.mu.
func (c *Controller) endDispatchFailureLocked(now time.Time, class string, cause error) {
	incidentID := c.currentIncidentRequest.IncidentID
	reason := class + ": " + cause.Error()
	c.responderDone = true
	c.responderCompletedAt = now
	if c.incidentDispatchedAt.IsZero() {
		// No worker ran. The closure needs a dispatch instant; the incident's own
		// start keeps the recovery clear-streak bound as early as the incident.
		c.incidentDispatchedAt = c.incidentDetectedAt
	}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.dispatchError = ""
	result := c.currentIncidentResult
	if result == nil {
		result = &IncidentResult{}
	}
	*result = IncidentResult{
		PlanApplications: result.PlanApplications, ProgramRuns: result.ProgramRuns, RepairActions: result.RepairActions,
		SchemaVersion: ProtocolSchemaVersion, IncidentID: incidentID, Status: IncidentFailed, Error: "responder dispatch failed: " + reason,
		Timing: TimingMetrics{StartedAt: c.incidentDispatchedAt, CompletedAt: now},
	}
	c.currentIncidentResult = result
	c.resetDispatchRetryLocked()
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseDispatchFailed, RecordedAt: contractTimestamp(now), IncidentId: incidentID, DispatchError: true, Reason: reason,
	})
	slog.Error("responder_dispatch_failed", "incident_id", incidentID, "class", class, "error", cause)
}

// dispatchPrepareFailure returns the preparation failure inside err, if any.
func dispatchPrepareFailure(err error) (*DispatchPrepareError, bool) {
	var prepare *DispatchPrepareError
	return prepare, errors.As(err, &prepare)
}
