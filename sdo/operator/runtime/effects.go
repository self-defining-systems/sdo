package runtime

import (
	"context"
	"errors"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"time"

	"google.golang.org/protobuf/proto"
)

// ErrEffectNotDurable reports an external side effect whose controller-state
// record has not been persisted. Executing it could leave a responder Job,
// worktree, or outcome commit that a restarted controller cannot recover.
var ErrEffectNotDurable = errors.New("controller effect is not durably recorded")

type DispatchEffect struct {
	Request IncidentRequest
}

func (c *Controller) PendingDispatchEffect() (DispatchEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || c.currentIncidentRequest == nil || c.earlyReportPendingLocked() {
		return DispatchEffect{}, false
	}
	if !c.dispatchRetryDueLocked() {
		return DispatchEffect{}, false
	}
	return DispatchEffect{Request: *cloneIncidentRequest(c.currentIncidentRequest)}, true
}

func (c *Controller) ExecuteDispatchEffect(ctx context.Context, effect DispatchEffect) error {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || c.currentIncidentRequest == nil {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("dispatch effect is not pending")
	}
	if effect.Request.IncidentID != c.currentIncidentRequest.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("dispatch effect incident %q does not match pending incident %q", effect.Request.IncidentID, c.currentIncidentRequest.IncidentID)
	}
	if err := c.requireDurableLocked("responder dispatch", func(state *contracts.ControllerState) bool {
		return state.IncidentOpen && state.DispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING && state.IncidentRequest != nil &&
			sameIncidentRequest(*incidentRequestState(state.IncidentRequest), effect.Request)
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	c.responderCancel = cancel
	c.responseStagesRunning = !(len(c.codePath.Selection.ResponseStages) == 1 && c.codePath.Selection.ResponseStages[0] == "agent")
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	if c.incidentDispatchedAt.IsZero() {
		c.incidentDispatchedAt = c.clock()
	}
	c.mu.Unlock()
	endBy := c.responseEndBound(effect.Request)
	c.goBackground(func() {
		defer cancel()
		dispatchCtx, endDispatch := context.WithTimeout(actionCtx, endBy)
		defer endDispatch()
		result, reports, dispatchErr := c.runResponsePipeline(dispatchCtx, effect.Request)
		if dispatchErr != nil && actionCtx.Err() == nil && errors.Is(dispatchCtx.Err(), context.DeadlineExceeded) {
			dispatchErr = c.endTimedOutResponse(actionCtx, effect.Request, dispatchErr)
		}
		c.results <- dispatchCompletion{incidentID: effect.Request.IncidentID, result: result, priorStages: reports, err: dispatchErr}
	})
	return nil
}

// responseEndGrace is how long past the response deadline the controller waits
// for a responder to answer before it ends the attempt itself. The worker stops at
// the deadline and the Job's own deadline follows shortly, so only a responder
// that ignores both reaches this bound.
const responseEndGrace = 30 * time.Second

// responseEndBound is how long the response pipeline may run: until the response
// deadline plus responseEndGrace. A dispatch that resumes past its deadline (a
// restarted controller rejoining a finished Job) still gets the grace to harvest.
func (c *Controller) responseEndBound(request IncidentRequest) time.Duration {
	remaining := time.Duration(0)
	if !request.ResponseDeadline.IsZero() {
		remaining = request.ResponseDeadline.Sub(c.clock())
	}
	return max(remaining, 0) + responseEndGrace
}

// ResponseTimeoutError is the cause of a responder attempt the controller ended
// at its response deadline.
type ResponseTimeoutError struct{ Deadline time.Time }

func (e *ResponseTimeoutError) Error() string {
	return fmt.Sprintf("responder did not answer by its response deadline %s; the controller ended the attempt", e.Deadline.UTC().Format(time.RFC3339))
}

// incidentWorkEnder stops the running responder work of an incident.
type incidentWorkEnder interface {
	EndIncidentWork(ctx context.Context, incidentID string) error
}

// endTimedOutResponse ends a responder that outlived its response deadline: it
// stops the dispatcher's work and returns the terminal failure the controller
// records for the attempt, the same as a responder Job that failed. The incident
// then closes by health alone, so it reaches an outcome and later incidents run.
func (c *Controller) endTimedOutResponse(ctx context.Context, request IncidentRequest, cause error) error {
	if ender, ok := c.dispatcher.(incidentWorkEnder); ok {
		endCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := ender.EndIncidentWork(endCtx, request.IncidentID); err != nil && c.OnError != nil {
			c.OnError(fmt.Errorf("end responder work of incident %s: %w", request.IncidentID, err))
		}
	}
	return &ResponderJobFailedError{JobName: IncidentJobName(request.IncidentID), Cause: errors.Join(&ResponseTimeoutError{Deadline: request.ResponseDeadline}, cause)}
}

// requireDurableLocked returns ErrEffectNotDurable unless the last durable
// state records the effect. Callers hold c.mu. Without a state store nothing
// survives a restart, so there is no record to wait for.
func (c *Controller) requireDurableLocked(effect string, recorded func(*contracts.ControllerState) bool) error {
	if c.stateStore == nil {
		return nil
	}
	if c.durable == nil || !recorded(c.durable) {
		return fmt.Errorf("%w: %s", ErrEffectNotDurable, effect)
	}
	return nil
}

// sameIncidentRequest compares requests by their persisted projection, which
// is what a restarted controller would recover.
func sameIncidentRequest(left IncidentRequest, right IncidentRequest) bool {
	leftMessage, leftErr := incidentRequestContract(left)
	rightMessage, rightErr := incidentRequestContract(right)
	return leftErr == nil && rightErr == nil && proto.Equal(leftMessage, rightMessage)
}

// sameIncidentClosure compares closures by their persisted projection.
func sameIncidentClosure(left IncidentClosure, right IncidentClosure) bool {
	leftMessage, leftErr := incidentClosureContract(left)
	rightMessage, rightErr := incidentClosureContract(right)
	return leftErr == nil && rightErr == nil && proto.Equal(leftMessage, rightMessage)
}
