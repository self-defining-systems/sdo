package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk"
	"sort"
	"strings"
	"time"
)

type WorkspaceEffect struct {
	IncidentID string
}

type ClosureEffect struct {
	Identity string
	Closure  IncidentClosure
}

// A committed outcome is historical evidence. Resuming its unfinished broker
// bookkeeping cannot authorize a new repair or another outcome classification.
type committedOutcomeClosureBroker interface {
	committedOutcomeClosure(context.Context, IncidentClosure, *ClosureReceipt) (bool, error)
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
	c.goBackground(func() {
		defer cancel()
		workspace, prepareErr := broker.PrepareIncident(actionCtx, effect.IncidentID)
		c.workspaceResults <- workspaceCompletion{workspace: workspace, err: prepareErr}
	})
	return nil
}

func (c *Controller) PendingClosureEffect() (ClosureEffect, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broker == nil || c.earlyReportPendingLocked() {
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
	// Receipt replay is bookkeeping for a closure already committed by the
	// native broker. Adoption can change observation generations, but must not
	// turn fetching that exact receipt into another repair/closure decision.
	var recovered *ClosureReceipt
	c.mu.Lock()
	configuredBroker := c.broker
	c.mu.Unlock()
	if proof, ok := configuredBroker.(committedRepairHealthBroker); ok && c.CheckHealth != nil {
		receipt, err := proof.committedRepairHealthReceipt(ctx, effect.Closure, nil)
		if err == nil {
			recovered = &receipt
		} else if !errors.Is(err, ErrStaleHealthInvestigation) {
			return err
		}
	}
	committedOutcome := false
	if proof, ok := configuredBroker.(committedOutcomeClosureBroker); ok {
		var err error
		committedOutcome, err = proof.committedOutcomeClosure(ctx, effect.Closure, nil)
		if err != nil {
			return err
		}
	}
	// Only closures without an accepted outcome can make a new success decision.
	// Scope supersession retires them durably; it is never a process-level error.
	var snapshot sdk.DetectionContext
	var verificationErr error
	if recovered == nil && !committedOutcome {
		var err error
		snapshot, err = c.provider.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("verify closure observation scope: %w", err)
		}
		if effect.Closure.ObservationGeneration != observationGeneration(snapshot) {
			verificationErr = fmt.Errorf("observation scope changed; pending closure %s is superseded and requires fresh verification", effect.Closure.Request.IncidentID)
		}
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	unconfirmed := ""
	if recovered == nil && !committedOutcome && verificationErr == nil {
		recheck := c.revalidateClosure(actionCtx, snapshot, effect.Closure)
		switch recheck.Verdict {
		case closureRecheckConfirmed:
			c.clearClosureRecheckWait()
		case closureRecheckContradicted:
			c.clearClosureRecheckWait()
			verificationErr = errors.New(recheck.Reason)
		case closureRecheckIncomplete:
			// "Not yet" is never a contradiction: wait for the window, bounded by
			// the detectors' own clearing horizons, then commit unconfirmed.
			if !c.closureRecheckWaitExpired(effect.Identity, recheck.Bound) {
				cancel()
				return nil
			}
			unconfirmed = recheck.Reason
		default:
			cancel()
			return fmt.Errorf("closure recheck returned unknown verdict %d", recheck.Verdict)
		}
	}
	if err := actionCtx.Err(); err != nil {
		cancel()
		return err
	}
	c.mu.Lock()
	if c.broker == nil || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING || c.pendingClosure == nil ||
		effect.Identity != lifecyclecore.Identity(c.pendingClosure.Request.IncidentID, c.pendingClosure.Request.RepositoryBaseCommit, lifecyclecore.ProcessClosure) ||
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
	if unconfirmed != "" {
		_, stepErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Unconfirmed, At: c.clock(), Error: unconfirmed})
		c.mu.Unlock()
		cancel()
		c.clearClosureRecheckWait()
		if stepErr != nil {
			return stepErr
		}
		slog.Warn("closure_unconfirmed", "incident_id", effect.Closure.Request.IncidentID, "reason", unconfirmed)
		// The next driver pass processes the unverified closure durably.
		return c.PersistState(ctx)
	}
	if verificationErr != nil {
		// Retirement uses the existing durable archive path. Do not reopen the
		// responder or reset its original verification deadline on each restart.
		_, retireErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Superseded, At: c.clock(), Error: verificationErr.Error()})
		failure := cloneClosureFailure(c.closureFailure)
		c.mu.Unlock()
		cancel()
		if retireErr != nil {
			return retireErr
		}
		if err := c.PersistState(ctx); err != nil {
			return err
		}
		if failure != nil {
			c.reportBrokerError("retire unverified closure", *failure)
			if c.OnClosureFailed != nil {
				c.OnClosureFailed(*failure)
			}
		}
		return nil
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
	c.goBackground(func() {
		defer cancel()
		var receipt ClosureReceipt
		var processErr error
		if recovered != nil {
			receipt = *recovered
		} else {
			receipt, processErr = broker.ProcessClosure(actionCtx, effect.Closure)
		}
		c.closureResults <- closureCompletion{effectID: effect.Identity, receipt: receipt, err: processErr}
	})
	return nil
}

// closureRecheckVerdict is the only result of the closure-time recheck, so a
// caller cannot collapse an incomplete observation into a contradiction.
type closureRecheckVerdict uint8

const (
	closureRecheckConfirmed closureRecheckVerdict = iota + 1
	// closureRecheckIncomplete is "not yet": a detector's observation window
	// is still filling. It never retires the closure.
	closureRecheckIncomplete
	// closureRecheckContradicted means health regressed, the repair did not
	// hold (active finding, unexplained inventory, unrestored source fields), or
	// the observation scope is lost (unavailable detector or observer).
	closureRecheckContradicted
)

type closureRecheck struct {
	Verdict closureRecheckVerdict
	Reason  string
	// Bound is how long an incomplete recheck may wait: the longest clearing
	// horizon of the incomplete detectors plus two of their cadences.
	Bound time.Duration
}

// revalidateClosure checks live dependencies and admitted source fields, rather
// than treating a matching policy generation as proof of continued recovery.
// Every detector is evaluated, so a contradiction dominates an incomplete peer.
func (c *Controller) revalidateClosure(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) closureRecheck {
	confirmed := closureRecheck{Verdict: closureRecheckConfirmed}
	if len(closure.FinalDetectorStates) == 0 || closure.DispatchError != "" {
		return confirmed // Already unverified; this closure cannot publish success.
	}
	contradicted := func(format string, args ...any) closureRecheck {
		return closureRecheck{Verdict: closureRecheckContradicted, Reason: fmt.Sprintf(format, args...)}
	}
	ids := map[string]bool{}
	for _, state := range closure.FinalDetectorStates {
		ids[state.DetectorID] = true
	}
	for _, id := range c.healthDetectorIDs {
		ids[id] = true
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	var incomplete []string
	var bound time.Duration
	wait := func(detector sdk.Detector, reason string) {
		incomplete = append(incomplete, reason)
		bound = max(bound, sdk.ClearingHorizon(detector, snapshot)+2*detector.Spec().Interval)
	}
	for _, id := range ordered {
		detector := c.detectors[id]
		if detector == nil && id == "drift.unexplained" {
			retained := []sdk.Finding{}
			for _, finding := range closure.Request.Findings {
				if finding.DetectorID == id {
					retained = append(retained, finding)
				}
			}
			if len(retained) == 0 || c.drift == nil || c.drift.View == nil {
				return contradicted("closure observation drift %s is unavailable", id)
			}

			continue // Admitted source fields are independently rechecked below.
		}
		if detector == nil {
			return contradicted("closure observation detector %s is unavailable", id)
		}
		findings, err := sdk.DetectObserved(ctx, detector, snapshot)
		if err != nil {
			// A denied or unavailable observer is a lost scope, not "not yet".
			return contradicted("closure observation detector %s: %v", id, err)
		}
		for _, finding := range findings {
			if finding.DetectorID == "" {
				finding.DetectorID = id
			}
			if len(finding.Playbooks) == 0 {
				finding.Playbooks = detector.Spec().Playbooks
			}
			if err := core.ValidateFinding(detector.Spec(), finding); err != nil {
				return contradicted("closure observation detector %s: %v", id, err)
			}
			if finding.Status == sdk.FindingActive {
				return contradicted("closure observation detector %s is active", id)
			}
		}
		if !detectorConfirmed(detector, snapshot) {
			wait(detector, fmt.Sprintf("closure observation detector %s is unconfirmed: %s", id, detectorConfirmationReason(detector, snapshot)))
		}
	}
	c.mu.Lock()
	inventoryClear, _ := c.verifySourceInventoryLocked(closure.Result)
	c.mu.Unlock()
	if !inventoryClear {
		return contradicted("closure accepted-source inventory remains unexplained")
	}
	for _, finding := range closure.Request.Findings {
		if inventoryFinding(finding) || !strings.HasPrefix(finding.DetectorID, "drift.") {
			continue
		}
		events := driftEventsForFindings(nil, []sdk.Finding{finding})
		if len(events) != 1 || len(events[0].Fields) == 0 {
			continue
		}
		restored, err := c.closureSourceRestored(ctx, snapshot, finding, events)
		if !restored {
			return contradicted("closure observation source fields for %s/%s are unverified: %v", finding.PrimaryResource.Kind, finding.PrimaryResource.Name, err)
		}
	}
	if len(incomplete) > 0 {
		return closureRecheck{Verdict: closureRecheckIncomplete, Reason: strings.Join(incomplete, "; "), Bound: bound}
	}
	return confirmed
}

// closureRecheckWaitExpired records when this closure's recheck first saw an
// incomplete window and reports whether the bound has passed. The wait is
// process-local: a restart re-observes and waits at most one more bound.
func (c *Controller) closureRecheckWaitExpired(identity string, bound time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if c.closureRecheckIdentity != identity || c.closureRecheckSince.IsZero() {
		c.closureRecheckIdentity, c.closureRecheckSince = identity, now
	}
	return now.Sub(c.closureRecheckSince) >= bound
}

func (c *Controller) clearClosureRecheckWait() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closureRecheckIdentity, c.closureRecheckSince = "", time.Time{}
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
	c.goBackground(func() {
		defer cancel()
		ackErr := broker.AcknowledgeClosure(actionCtx, effect.Receipt)
		c.acknowledgmentResults <- acknowledgmentCompletion{effectID: effect.Identity, err: ackErr}
	})
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
		var terminal interface{ NonRetryable() bool }
		event.NonRetryable = errors.As(completion.err, &terminal) && terminal.NonRetryable()
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
