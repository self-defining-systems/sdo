package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrEffectNotDurable reports an external side effect whose controller-state
// record has not been persisted. Executing it could leave a responder Job,
// worktree, or outcome commit that a restarted controller cannot recover.
var ErrEffectNotDurable = errors.New("controller effect is not durably recorded")

type DispatchEffect struct {
	Request IncidentRequest `json:"request"`
}

func (c *Controller) PendingDispatchEffect() (DispatchEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dispatchState != "pending" || c.currentIncidentRequest == nil {
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
	if c.dispatchState != "pending" || c.currentIncidentRequest == nil {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("dispatch effect is not pending")
	}
	if effect.Request.IncidentID != c.currentIncidentRequest.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("dispatch effect incident %q does not match pending incident %q", effect.Request.IncidentID, c.currentIncidentRequest.IncidentID)
	}
	if err := c.requireDurableLocked("responder dispatch", func(state RuntimeState) bool {
		return state.IncidentOpen && state.DispatchState == "pending" && state.IncidentRequest != nil &&
			sameJSON(*state.IncidentRequest, effect.Request)
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	c.dispatchState = "running"
	if c.incidentDispatchedAt.IsZero() {
		c.incidentDispatchedAt = c.incidentDetectedAt
	}
	c.emitLifecycleLocked(LifecycleEvent{
		Event: PhaseDispatched, RecordedAt: c.incidentDispatchedAt, IncidentID: effect.Request.IncidentID,
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
func (c *Controller) requireDurableLocked(effect string, recorded func(RuntimeState) bool) error {
	if c.stateStore == nil {
		return nil
	}
	if c.durable == nil || !recorded(*c.durable) {
		return fmt.Errorf("%w: %s", ErrEffectNotDurable, effect)
	}
	return nil
}

// sameJSON compares values by their persisted encoding, which is what a
// restarted controller would recover.
func sameJSON(left any, right any) bool {
	leftPayload, leftErr := json.Marshal(left)
	rightPayload, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftPayload, rightPayload)
}
