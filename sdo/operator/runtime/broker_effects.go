package runtime

import (
	"context"
	"fmt"
)

type WorkspaceEffect struct {
	IncidentID string `json:"incident_id"`
}

type ClosureEffect struct {
	Closure IncidentClosure `json:"closure"`
}

type ClosureAcknowledgmentEffect struct {
	Receipt ClosureReceipt `json:"receipt"`
}

func (c *Controller) PendingWorkspaceEffect() (WorkspaceEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.dispatchState != "workspace_pending" || c.currentIncidentRequest == nil {
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
	if c.broker == nil || c.dispatchState != "workspace_pending" || c.currentIncidentRequest == nil ||
		c.currentIncidentRequest.IncidentID != effect.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("workspace effect is not pending")
	}
	if err := c.requireDurableLocked("incident worktree preparation", func(state RuntimeState) bool {
		return state.IncidentOpen && state.DispatchState == "workspace_pending" && state.IncidentRequest != nil &&
			state.IncidentRequest.IncidentID == effect.IncidentID
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	c.dispatchState = "workspace_running"
	c.mu.Unlock()
	go func() {
		defer cancel()
		workspace, prepareErr := broker.PrepareIncident(actionCtx, effect.IncidentID)
		c.workspaceResults <- workspaceCompletion{workspace: workspace, err: prepareErr}
	}()
	return nil
}

type ReleaseEffect struct {
	IncidentID string `json:"incident_id"`
}

// PendingReleaseEffect reports the next stranded worktree to reap. Releases are
// serialized through releaseInFlight so the pump drives one at a time, matching
// the single-in-flight shape of the other broker effects.
func (c *Controller) PendingReleaseEffect() (ReleaseEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.releaseInFlight != "" || len(c.pendingReleases) == 0 {
		return ReleaseEffect{}, false
	}
	return ReleaseEffect{IncidentID: c.pendingReleases[0]}, true
}

func (c *Controller) ExecuteReleaseEffect(ctx context.Context, effect ReleaseEffect) error {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.releaseInFlight != "" || len(c.pendingReleases) == 0 ||
		c.pendingReleases[0] != effect.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("release effect is not pending")
	}
	if err := c.requireDurableLocked("incident worktree release", func(state RuntimeState) bool {
		for _, id := range state.PendingReleases {
			if id == effect.IncidentID {
				return true
			}
		}
		return false
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	c.releaseInFlight = effect.IncidentID
	c.mu.Unlock()
	go func() {
		defer cancel()
		releaseErr := broker.ReleaseIncident(actionCtx, effect.IncidentID)
		c.releaseResults <- releaseCompletion{incidentID: effect.IncidentID, err: releaseErr}
	}()
	return nil
}

func (c *Controller) PendingClosureEffect() (ClosureEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.closureState != "pending" || c.pendingClosure == nil || !c.closureRetryDueLocked() {
		return ClosureEffect{}, false
	}
	return ClosureEffect{Closure: *cloneIncidentClosure(c.pendingClosure)}, true
}

func (c *Controller) ExecuteClosureEffect(ctx context.Context, effect ClosureEffect) error {
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.closureState != "pending" || c.pendingClosure == nil ||
		c.pendingClosure.Request.IncidentID != effect.Closure.Request.IncidentID {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure effect is not pending")
	}
	if err := c.requireDurableLocked("incident closure", func(state RuntimeState) bool {
		return state.ClosureState == "pending" && state.PendingClosure != nil &&
			sameJSON(*state.PendingClosure, effect.Closure)
	}); err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	broker := c.broker
	c.closureState = "processing"
	c.mu.Unlock()
	go func() {
		defer cancel()
		receipt, processErr := broker.ProcessClosure(actionCtx, effect.Closure)
		c.closureResults <- closureCompletion{receipt: receipt, err: processErr}
	}()
	return nil
}

func (c *Controller) PendingClosureAcknowledgmentEffect() (ClosureAcknowledgmentEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.closureState != "committed" || c.closureReceipt == nil {
		return ClosureAcknowledgmentEffect{}, false
	}
	return ClosureAcknowledgmentEffect{Receipt: *cloneClosureReceipt(c.closureReceipt)}, true
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
	if c.broker == nil || c.closureState != "committed" || c.closureReceipt == nil ||
		c.closureReceipt.AckToken != effect.Receipt.AckToken {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("closure acknowledgment effect is not pending")
	}
	if err := c.requireDurableLocked("closure acknowledgment", func(state RuntimeState) bool {
		return state.ClosureState == "committed" && state.ClosureReceipt != nil &&
			*state.ClosureReceipt == effect.Receipt
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
	c.closureState = "acknowledging"
	c.mu.Unlock()
	go func() {
		defer cancel()
		ackErr := broker.AcknowledgeClosure(actionCtx, effect.Receipt)
		c.acknowledgmentResults <- acknowledgmentCompletion{err: ackErr}
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
			goto releases
		}
	}

releases:
	for {
		select {
		case completion := <-c.releaseResults:
			c.handleReleaseCompletion(completion)
		default:
			return
		}
	}
}

func (c *Controller) handleReleaseCompletion(completion releaseCompletion) {
	c.mu.Lock()
	c.releaseInFlight = ""
	if completion.err != nil {
		// Leave the incident queued so a later pass retries it; release is
		// idempotent so a retry cannot double-reap a live worktree.
		c.mu.Unlock()
		c.reportBrokerError("release incident worktree", completion.err)
		return
	}
	kept := c.pendingReleases[:0]
	for _, id := range c.pendingReleases {
		if id != completion.incidentID {
			kept = append(kept, id)
		}
	}
	c.pendingReleases = kept
	c.emitLifecycleLocked(LifecycleEvent{
		Event: PhaseWorktreeReleased, RecordedAt: c.clock(), IncidentID: completion.incidentID,
		Reason: "superseded",
	})
	c.mu.Unlock()
}

func (c *Controller) handleWorkspaceCompletion(completion workspaceCompletion) {
	c.mu.Lock()
	if completion.err != nil {
		c.dispatchState = "workspace_pending"
		c.mu.Unlock()
		c.reportBrokerError("prepare incident worktree", completion.err)
		return
	}
	if c.currentIncidentRequest == nil || completion.workspace.IncidentID != c.currentIncidentRequest.IncidentID ||
		completion.workspace.Worktree == "" || completion.workspace.BaseCommit == "" {
		c.dispatchState = "workspace_pending"
		c.mu.Unlock()
		c.reportBrokerError("prepare incident worktree", fmt.Errorf("broker returned invalid workspace"))
		return
	}
	c.currentIncidentRequest.RepositoryWorktree = completion.workspace.Worktree
	c.currentIncidentRequest.RepositoryBaseCommit = completion.workspace.BaseCommit
	c.dispatchState = "pending"
	c.emitLifecycleLocked(LifecycleEvent{
		Event: PhaseWorkspacePrepared, RecordedAt: c.clock(), IncidentID: completion.workspace.IncidentID,
		Worktree: completion.workspace.Worktree, BaseCommit: completion.workspace.BaseCommit,
	})
	c.mu.Unlock()
}

func (c *Controller) handleClosureCompletion(completion closureCompletion) {
	c.mu.Lock()
	err := completion.err
	if err == nil && (c.pendingClosure == nil || completion.receipt.IncidentID != c.pendingClosure.Request.IncidentID ||
		completion.receipt.OutcomeCommit == "" || completion.receipt.AckToken == "") {
		err = fmt.Errorf("broker returned invalid closure receipt")
	}
	if err != nil {
		if c.pendingClosure == nil {
			c.closureState = "pending"
			c.mu.Unlock()
			c.reportBrokerError("process incident closure", err)
			return
		}
		failure, permanent := c.recordClosureRejectionLocked(err)
		c.mu.Unlock()
		c.reportBrokerError("process incident closure", err)
		if permanent {
			c.reportBrokerError("give up incident closure", failure)
			if c.OnClosureFailed != nil {
				c.OnClosureFailed(failure)
			}
		}
		return
	}
	c.closureReceipt = cloneClosureReceipt(&completion.receipt)
	c.config.SourceCommit = completion.receipt.OutcomeCommit
	if completion.receipt.ReflectionCommit != "" {
		c.config.SourceCommit = completion.receipt.ReflectionCommit
	}
	c.closureFailure = nil
	c.closureState = "committed"
	c.emitLifecycleLocked(LifecycleEvent{
		Event: PhaseClosureCommitted, RecordedAt: c.clock(), IncidentID: completion.receipt.IncidentID,
		Worktree: completion.receipt.Worktree, BaseCommit: completion.receipt.BaseCommit,
		ProposalCommit: completion.receipt.ProposalCommit, OutcomeCommit: completion.receipt.OutcomeCommit,
		ReflectionCommit: completion.receipt.ReflectionCommit,
	})
	c.mu.Unlock()
}

func (c *Controller) handleAcknowledgmentCompletion(completion acknowledgmentCompletion) {
	c.mu.Lock()
	if completion.err != nil {
		c.closureState = "committed"
		c.mu.Unlock()
		c.reportBrokerError("acknowledge incident closure", completion.err)
		return
	}
	if c.closureReceipt != nil {
		c.lastAcknowledgedIncidentID = c.closureReceipt.IncidentID
		c.emitLifecycleLocked(LifecycleEvent{
			Event: PhaseAcknowledged, RecordedAt: c.clock(), IncidentID: c.closureReceipt.IncidentID,
			OutcomeCommit: c.closureReceipt.OutcomeCommit,
		})
	}
	c.pendingClosure = nil
	c.closureReceipt = nil
	c.closureState = ""
	// Any activation buffered before or during this incident is represented by
	// the acknowledged closure. Drop it so crash restoration cannot redispatch
	// the same evidence; a genuinely recurring fault must clear and reactivate.
	c.batcher.Drain()
	c.mu.Unlock()
}

func (c *Controller) reportBrokerError(operation string, err error) {
	if c.OnError != nil {
		c.OnError(fmt.Errorf("%s: %w", operation, err))
	}
}
