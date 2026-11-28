package runtime

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// These are SDK/proto projections at the driver edge, not transition policy.
// Callers hold c.mu. Only lifecyclecore.Step changes closure lifecycle state.
func (c *Controller) closureCoreStateLocked() (*contracts.ControllerState, error) {
	state := &contracts.ControllerState{ClosureState: c.closureState, ClosureReceipt: closureReceiptContract(c.closureReceipt), ClosureFailure: closureFailureContract(c.closureFailure), FailedClosures: c.failedClosures, LastAcknowledgedIncidentId: c.lastAcknowledgedIncidentID, LastAcknowledgedExternalIncidentId: c.lastAcknowledgedExternalIncidentID}
	if c.pendingClosure != nil {
		var err error
		state.PendingClosure, err = incidentClosureContract(*c.pendingClosure)
		if err != nil {
			return nil, fmt.Errorf("project closure transition: %w", err)
		}
	}
	return state, nil
}

func (c *Controller) applyClosureCoreLocked(state *contracts.ControllerState) {
	c.closureState = state.ClosureState
	c.closureReceipt = closureReceiptState(state.ClosureReceipt)
	c.closureFailure = closureFailureState(state.ClosureFailure)
	c.pendingClosure = nil
	if state.PendingClosure != nil {
		closure := incidentClosureState(state.PendingClosure)
		c.pendingClosure = &closure
	}
	c.failedClosures = state.FailedClosures
	c.lastAcknowledgedIncidentID = state.LastAcknowledgedIncidentId
	c.lastAcknowledgedExternalIncidentID = state.LastAcknowledgedExternalIncidentId
}

// errUnconfirmedNoop reports an Unconfirmed step that left the closure
// unchanged: the closure makes no health claim to withdraw.
var errUnconfirmedNoop = errors.New("unconfirmed decision changed nothing: the closure makes no health claim to withdraw, so it commits as recorded")

func (c *Controller) stepClosureLocked(event lifecyclecore.Event) ([]lifecyclecore.Effect, error) {
	if event.Kind == lifecyclecore.Begin && event.Closure != nil && !event.Closure.GetVerifiedWhileResponderRunning() {
		if err := c.bindClosureUsage(event.Closure); err != nil {
			return nil, err
		}
	}
	state, err := c.closureCoreStateLocked()
	if err != nil {
		return nil, err
	}
	event.Policy = lifecyclecore.RetryPolicy{MaxAttempts: int32(c.config.ClosureRetry.MaxAttempts), InitialBackoff: c.config.ClosureRetry.InitialBackoff, MaxBackoff: c.config.ClosureRetry.MaxBackoff}
	next, effects := lifecyclecore.Step(state, event)
	if event.Kind == lifecyclecore.Unconfirmed && proto.Equal(state, next) {
		// The caller decided the closure is unconfirmed. A reducer that
		// ignored that decision would leave the closure pending, to be
		// rechecked and decided again on every pass.
		incident := ""
		if c.pendingClosure != nil {
			incident = c.pendingClosure.Request.IncidentID
		}
		return nil, fmt.Errorf("%w: incident %q (%s)", errUnconfirmedNoop, incident, event.Error)
	}
	c.applyClosureCoreLocked(next)
	c.publishHeldLocked()
	return effects, nil
}

func (c *Controller) closureEffectsLocked() ([]lifecyclecore.Effect, error) {
	state, err := c.closureCoreStateLocked()
	if err != nil {
		return nil, err
	}
	_, effects := lifecyclecore.Step(state, lifecyclecore.Event{Kind: lifecyclecore.Drive, At: c.clock()})
	return effects, nil
}

func (c *Controller) closureBlocksNextLocked() bool {
	return lifecyclecore.BlocksNext(c.closureState, c.pendingClosure != nil)
}

func cloneFailedClosures(records []*contracts.FailedIncidentClosure) []*contracts.FailedIncidentClosure {
	cloned := make([]*contracts.FailedIncidentClosure, len(records))
	for i, record := range records {
		cloned[i] = proto.Clone(record).(*contracts.FailedIncidentClosure)
	}
	return cloned
}
