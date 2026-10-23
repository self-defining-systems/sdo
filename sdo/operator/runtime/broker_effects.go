package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

type WorkspaceEffect struct {
	IncidentID string
}

type ClosureEffect struct {
	Identity string
	Closure  IncidentClosure
}

type ClosureAcknowledgmentEffect struct {
	Identity string
	Receipt  ClosureReceipt
}

func (c *Controller) PendingWorkspaceEffect() (WorkspaceEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.dispatchState != contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING || c.currentIncidentRequest == nil {
		return WorkspaceEffect{}, false
	}
	return WorkspaceEffect{IncidentID: c.currentIncidentRequest.IncidentID}, true
}

func (c *Controller) ExecuteWorkspaceEffect(ctx context.Context, effect WorkspaceEffect) error {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.dispatchState != contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING || c.currentIncidentRequest == nil ||
		c.currentIncidentRequest.IncidentID != effect.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("workspace effect is not pending")
	}
	if err := c.requireDurableLocked("incident worktree preparation", func(state *contracts.ControllerState) bool {
		return state.IncidentOpen && state.DispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING && state.IncidentRequest != nil &&
			state.IncidentRequest.IncidentId == effect.IncidentID
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING
	c.mu.Unlock()
	go func() {
		defer cancel()
		workspace, prepareErr := broker.PrepareIncident(actionCtx, effect.IncidentID)
		c.workspaceResults <- workspaceCompletion{workspace: workspace, err: prepareErr}
	}()
	return nil
}

func (c *Controller) PendingClosureEffect() (ClosureEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil {
		return ClosureEffect{}, false
	}
	effects, err := c.closureEffectsLocked()
	if err != nil {
		return ClosureEffect{}, false
	}
	for _, effect := range effects {
		if effect.Kind == lifecyclecore.ProcessClosure {
			return ClosureEffect{Identity: effect.ID, Closure: incidentClosureState(effect.Closure)}, true
		}
	}
	return ClosureEffect{}, false
}

func (c *Controller) ExecuteClosureEffect(ctx context.Context, effect ClosureEffect) error {
	// Always refresh accepted scope here: restore and leadership adoption can
	// retain a success verified before a policy update, even without a Step.
	snapshot, err := c.provider.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("verify closure observation scope: %w", err)
	}
	if effect.Closure.ObservationGeneration != observationGeneration(snapshot) {
		return fmt.Errorf("observation scope changed; pending closure %s is superseded and requires fresh verification", effect.Closure.Request.IncidentID)
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING || c.pendingClosure == nil ||
		c.pendingClosure.Request.IncidentID != effect.Closure.Request.IncidentID || !sameIncidentClosure(*c.pendingClosure, effect.Closure) {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure effect is not pending")
	}
	if err := c.requireDurableLocked("incident closure", func(state *contracts.ControllerState) bool {
		return state.ClosureState == contracts.ClosureState_CLOSURE_STATE_PENDING && state.PendingClosure != nil &&
			sameIncidentClosure(incidentClosureState(state.PendingClosure), effect.Closure)
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	effects, stepErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Drive, At: c.clock(), EffectID: effect.Identity})
	if stepErr != nil || len(effects) != 1 || effects[0].ID != effect.Identity {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure effect changed before execution: %v", stepErr)
	}
	c.mu.Unlock()
	slog.Info("closure_handoff", "incident_id", effect.Closure.Request.IncidentID, "verified_to_handoff_ms", c.clock().Sub(effect.Closure.VerifiedAt).Milliseconds())
	go func() {
		defer cancel()
		receipt, processErr := broker.ProcessClosure(actionCtx, effect.Closure)
		c.closureResults <- closureCompletion{effectID: effect.Identity, receipt: receipt, err: processErr}
	}()
	return nil
}

func (c *Controller) PendingClosureAcknowledgmentEffect() (ClosureAcknowledgmentEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil {
		return ClosureAcknowledgmentEffect{}, false
	}
	effects, err := c.closureEffectsLocked()
	if err != nil {
		return ClosureAcknowledgmentEffect{}, false
	}
	for _, effect := range effects {
		if effect.Kind == lifecyclecore.AcknowledgeClosure {
			return ClosureAcknowledgmentEffect{Identity: effect.ID, Receipt: *closureReceiptState(effect.Receipt)}, true
		}
	}
	return ClosureAcknowledgmentEffect{}, false
}

func (c *Controller) ExecuteClosureAcknowledgmentEffect(
	ctx context.Context,
	effect ClosureAcknowledgmentEffect,
) error {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || c.closureReceipt == nil ||
		*c.closureReceipt != effect.Receipt {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure acknowledgment effect is not pending")
	}
	if err := c.requireDurableLocked("closure acknowledgment", func(state *contracts.ControllerState) bool {
		return state.ClosureState == contracts.ClosureState_CLOSURE_STATE_COMMITTED && state.ClosureReceipt != nil &&
			*closureReceiptState(state.ClosureReceipt) == effect.Receipt
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	if rolloutBroker, ok := broker.(rolloutPendingBroker); ok {
		pending, err := rolloutBroker.RolloutPending(effect.Receipt)
		if err != nil {
			c.mu.Unlock()
			cancel()
			return fmt.Errorf("check controller rollout before acknowledgment: %w", err)
		}
		if pending {
			c.mu.Unlock()
			cancel()
			return ErrControllerRolloutRestartRequired
		}
	}
	effects, stepErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Drive, At: c.clock(), EffectID: effect.Identity})
	if stepErr != nil || len(effects) != 1 || effects[0].ID != effect.Identity {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure acknowledgment changed before execution: %v", stepErr)
	}
	c.mu.Unlock()
	go func() {
		defer cancel()
		ackErr := broker.AcknowledgeClosure(actionCtx, effect.Receipt)
		c.acknowledgmentResults <- acknowledgmentCompletion{effectID: effect.Identity, err: ackErr}
	}()
	return nil
}

func (c *Controller) processBrokerCompletions() {
	for {
		select {
		case completion := <-c.workspaceResults:
			c.handleWorkspaceCompletion(completion)
		default:
			goto closures
		}
	}

closures:
	for {
		select {
		case completion := <-c.closureResults:
			c.handleClosureCompletion(completion)
		default:
			goto acknowledgments
		}
	}

acknowledgments:
	for {
		select {
		case completion := <-c.acknowledgmentResults:
			c.handleAcknowledgmentCompletion(completion)
		default:
			return
		}
	}
}

func (c *Controller) handleWorkspaceCompletion(completion workspaceCompletion) {
	c.mu.Lock()
	if completion.err != nil {
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
		c.mu.Unlock()
		c.reportBrokerError("prepare incident worktree", completion.err)
		return
	}
	if c.currentIncidentRequest == nil || completion.workspace.IncidentID != c.currentIncidentRequest.IncidentID ||
		completion.workspace.Worktree == "" || completion.workspace.BaseCommit == "" {
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING
		c.mu.Unlock()
		c.reportBrokerError("prepare incident worktree", fmt.Errorf("broker returned invalid workspace"))
		return
	}
	c.currentIncidentRequest.RepositoryWorktree = completion.workspace.Worktree
	c.currentIncidentRequest.RepositoryBaseCommit = completion.workspace.BaseCommit
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseWorkspacePrepared, RecordedAt: contractTimestamp(c.clock()), IncidentId: completion.workspace.IncidentID,
		Worktree: completion.workspace.Worktree, BaseCommit: completion.workspace.BaseCommit,
	})
	c.mu.Unlock()
}

func (c *Controller) handleClosureCompletion(completion closureCompletion) {
	c.mu.Lock()
	if (c.closureState != contracts.ClosureState_CLOSURE_STATE_PROCESSING && c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING) || c.pendingClosure == nil || completion.effectID != lifecyclecore.Identity(c.pendingClosure.Request.IncidentID, c.pendingClosure.Request.RepositoryBaseCommit, lifecyclecore.ProcessClosure) {
		c.mu.Unlock()
		return
	}
	event := lifecyclecore.Event{Kind: lifecyclecore.Committed, At: c.clock(), EffectID: completion.effectID, Receipt: closureReceiptContract(&completion.receipt)}
	if completion.err != nil {
		event.Kind = lifecyclecore.Rejected
		event.Error = completion.err.Error()
		if errors.Is(completion.err, context.DeadlineExceeded) {
			event.Kind = lifecyclecore.AgentTimeout
		}
		if errors.Is(completion.err, context.Canceled) {
			event.Kind = lifecyclecore.Canceled
		}
	}
	_, err := c.stepClosureLocked(event)
	if err != nil {
		c.mu.Unlock()
		c.reportBrokerError("closure transition", err)
		return
	}
	if c.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED {
		failure := cloneClosureFailure(c.closureFailure)
		c.mu.Unlock()
		if completion.err != nil {
			c.reportBrokerError("process incident closure", completion.err)
		} else if failure != nil {
			c.reportBrokerError("process incident closure", fmt.Errorf("%s", failure.LastError))
		}
		if failure != nil && failure.Permanent {
			c.reportBrokerError("give up incident closure", *failure)
			if c.OnClosureFailed != nil {
				c.OnClosureFailed(*failure)
			}
		}
		return
	}
	c.config.SourceCommit = completion.receipt.OutcomeCommit
	if completion.receipt.ReflectionCommit != "" {
		c.config.SourceCommit = completion.receipt.ReflectionCommit
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseClosureCommitted, RecordedAt: contractTimestamp(c.clock()), IncidentId: completion.receipt.IncidentID, Worktree: completion.receipt.Worktree, BaseCommit: completion.receipt.BaseCommit, ProposalCommit: completion.receipt.ProposalCommit, OutcomeCommit: completion.receipt.OutcomeCommit, ReflectionCommit: completion.receipt.ReflectionCommit})
	c.mu.Unlock()
}

func (c *Controller) handleAcknowledgmentCompletion(completion acknowledgmentCompletion) {
	c.mu.Lock()
	if c.closureState != contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING || c.closureReceipt == nil || completion.effectID != lifecyclecore.Identity(c.closureReceipt.IncidentID, c.closureReceipt.OutcomeCommit+"\x00"+c.closureReceipt.ReflectionCommit, lifecyclecore.AcknowledgeClosure) {
		c.mu.Unlock()
		return
	}
	event := lifecyclecore.Event{Kind: lifecyclecore.Acknowledged, At: c.clock(), EffectID: completion.effectID}
	if completion.err != nil {
		event.Kind = lifecyclecore.AcknowledgmentRejected
	}
	receipt := cloneClosureReceipt(c.closureReceipt)
	pending := cloneIncidentClosure(c.pendingClosure)
	_, err := c.stepClosureLocked(event)
	if err != nil {
		c.mu.Unlock()
		c.reportBrokerError("acknowledgment transition", err)
		return
	}
	if completion.err != nil {
		c.mu.Unlock()
		c.reportBrokerError("acknowledge incident closure", completion.err)
		return
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseAcknowledged, RecordedAt: contractTimestamp(c.clock()), IncidentId: receipt.IncidentID, OutcomeCommit: receipt.OutcomeCommit})
	if pending != nil {
		c.batcher.RemoveKeys(findingKeys(pending.Request.Findings))
	}
	c.mu.Unlock()
}

func (c *Controller) reportBrokerError(operation string, err error) {
	if c.OnError != nil {
		c.OnError(fmt.Errorf("%s: %w", operation, err))
	}
}
