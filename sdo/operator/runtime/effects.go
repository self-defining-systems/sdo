package runtime

import (
	"context"
	"errors"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

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
	if c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING || c.currentIncidentRequest == nil {
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
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	if c.incidentDispatchedAt.IsZero() {
		c.incidentDispatchedAt = c.incidentDetectedAt
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseDispatched, RecordedAt: contractTimestamp(c.incidentDispatchedAt), IncidentId: effect.Request.IncidentID,
		Worktree: effect.Request.RepositoryWorktree, BaseCommit: effect.Request.RepositoryBaseCommit,
	})
	c.mu.Unlock()
	go func() {
		defer cancel()
		result, dispatchErr := c.dispatcher.Dispatch(actionCtx, effect.Request)
		c.results <- dispatchCompletion{incidentID: effect.Request.IncidentID, result: result, err: dispatchErr}
	}()
	return nil
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
