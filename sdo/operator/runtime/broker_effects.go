package runtime

import (
	"context"
	"errors"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"log/slog"
	"net"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk"
	"slices"
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

// committedClosureBroker recovers the receipt of an already committed closure,
// so a replay fetches it instead of deciding the closure again.
type committedClosureBroker interface {
	committedClosureReceipt(context.Context, IncidentClosure, *ClosureReceipt) (ClosureReceipt, bool, error)
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
	if proof, ok := configuredBroker.(committedClosureBroker); ok {
		receipt, found, err := proof.committedClosureReceipt(ctx, effect.Closure, nil)
		if err != nil {
			return err
		}
		if found {
			recovered = &receipt
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
	// The recheck runs under the detectors now in force, whatever scope the
	// incident began with.
	var snapshot sdk.DetectionContext
	var verificationErr error
	if recovered == nil && !committedOutcome {
		var err error
		snapshot, err = c.provider.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("verify closure observation scope: %w", err)
		}
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	unconfirmed, denied := "", ""
	var unconfirmedReason contracts.ClosureUnconfirmedReason
	closure := effect.Closure
	if recovered == nil && !committedOutcome && verificationErr == nil {
		recheck := c.revalidateClosure(actionCtx, snapshot, effect.Closure)
		closure.SourceCheckNotes = append(closure.SourceCheckNotes, recheck.DriftNotes...)
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
			if recheck.SourceOnly {
				// Health alone verified the closure; the source checks that
				// never became available are information on the outcome.
				c.clearClosureRecheckWait()
				closure.SourceCheckNotes = append(closure.SourceCheckNotes, recheck.SourceNotes...)
				slog.Warn("closure_source_check_unavailable", "incident_id", closure.Request.IncidentID, "notes", recheck.SourceNotes)
				break
			}
			unconfirmed, unconfirmedReason = recheck.Reason, recheck.Unconfirmed
		case closureRecheckDenied:
			c.clearClosureRecheckWait()
			denied = recheck.Reason
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
		_, stepErr := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Unconfirmed, At: c.clock(), Error: unconfirmed, UnconfirmedReason: unconfirmedReason})
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
	if verificationErr != nil || denied != "" {
		// Retirement uses the existing durable archive path. Do not reopen the
		// responder or reset its original verification deadline on each restart.
		event := lifecyclecore.Event{Kind: lifecyclecore.ObserverDenied, At: c.clock(), Error: denied}
		if verificationErr != nil {
			event = lifecyclecore.Event{Kind: lifecyclecore.Superseded, At: c.clock(), Error: verificationErr.Error()}
		}
		_, retireErr := c.stepClosureLocked(event)
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
			receipt, processErr = broker.ProcessClosure(actionCtx, closure)
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
	// closureRecheckIncomplete is "not yet" or "unknown": a detector's
	// observation window is still filling, or its observer is unavailable
	// (timeout, API 5xx, unsynced). It never retires the closure.
	closureRecheckIncomplete
	// closureRecheckDenied means the recheck is denied its observation scope
	// (RBAC, undeclared or unsupported dependency): a misconfiguration that
	// fails the closure with a typed reason, never a contradicted repair.
	closureRecheckDenied
	// closureRecheckContradicted means the repair did not hold: an active
	// finding, unexplained inventory, unrestored source fields, or a detector
	// the adopted policy no longer has.
	closureRecheckContradicted
)

type closureRecheck struct {
	Verdict closureRecheckVerdict
	Reason  string
	// Unconfirmed is the typed reason an incomplete recheck commits with at
	// its bound: an unavailable observer outranks an incomplete window.
	Unconfirmed contracts.ClosureUnconfirmedReason
	// Bound is how long an incomplete recheck may wait: the longest clearing
	// horizon of the incomplete detectors plus two of their cadences.
	Bound time.Duration
	// Skipped lists incident detectors the adopted policy no longer has. They
	// are not evidence either way; the detectors in force decided.
	Skipped []string
	// SourceNotes lists source checks that could not run (the accepted source
	// would not render, the inventory is not loaded, the dry-run failed).
	// They are information: they wait for the bound, never contradict, and
	// never downgrade a closure that health alone has verified.
	SourceNotes []string
	// SourceOnly marks an incomplete recheck whose only gap is SourceNotes.
	SourceOnly bool
	// DriftNotes lists objects that still differ from the accepted source.
	// They are recorded on the outcome and never hold or contradict a closure.
	DriftNotes []string
}

// driftNote describes one object that still differs from the accepted source.
func driftNote(finding sdk.Finding) string {
	return fmt.Sprintf("unexplained drift remains on %s/%s: %s", finding.PrimaryResource.Kind, finding.PrimaryResource.Name, finding.Summary)
}

// closureInventoryLoadBound is how long a closure waits for the accepted-source
// checks (inventory, field comparison) to become available before it commits
// on health alone, with the unavailable checks noted.
const closureInventoryLoadBound = 30 * time.Second

// revalidateClosure checks live dependencies and admitted source fields, rather
// than treating a matching policy generation as proof of continued recovery.
// Every detector is evaluated, so a contradiction dominates an incomplete peer.
// Source and render state are information only: an unavailable source check
// waits and is noted, and only an observed difference contradicts.
func (c *Controller) revalidateClosure(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) closureRecheck {
	confirmed := closureRecheck{Verdict: closureRecheckConfirmed}
	if len(closure.FinalDetectorStates) == 0 || closure.DispatchError != "" {
		// Already unverified or without detector states to recheck; the outcome
		// still records what differs from the accepted source.
		driftNotes, _, _ := c.closureDriftNotes(ctx, snapshot, closure)
		confirmed.DriftNotes = driftNotes
		return confirmed
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
	var incomplete, sourceNotes []string
	var bound, sourceBound time.Duration
	unconfirmed := contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE
	wait := func(detector sdk.Detector, reason string) {
		incomplete = append(incomplete, reason)
		bound = max(bound, sdk.ClearingHorizon(detector, snapshot)+2*detector.Spec().Interval)
	}
	var denied, gone []string
	evaluated := 0
	for _, id := range ordered {
		detector := c.detectors[id]
		if detector == nil && !slices.Contains(c.healthDetectorIDs, id) {
			// The adopted policy no longer has this incident detector. The
			// repository moving is not evidence about the cluster; the
			// detectors in force still decide, provided one remains.
			gone = append(gone, id)
			continue
		}
		if detector == nil {
			return contradicted("closure observation detector %s is unavailable", id)
		}
		evaluated++
		findings, err := sdk.DetectObserved(ctx, detector, snapshot)
		if err != nil {
			reason := fmt.Sprintf("closure observation detector %s: %v", id, err)
			if observerDenied(detector, snapshot, err) {
				denied = append(denied, reason)
			} else {
				wait(detector, reason)
				unconfirmed = contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE
			}
			continue
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
	if len(gone) > 0 && evaluated == 0 {
		return contradicted("closure observation detector %s is unavailable", gone[0])
	}
	if len(gone) > 0 {
		slog.Warn("closure_detector_skipped", "incident_id", closure.Request.IncidentID, "detectors", gone, "reason", "removed by the adopted policy")
	}
	driftNotes, sourceNotes, sourceBound := c.closureDriftNotes(ctx, snapshot, closure)
	if len(denied) > 0 {
		return closureRecheck{Verdict: closureRecheckDenied, Reason: strings.Join(denied, "; "), Skipped: gone}
	}
	if len(incomplete) > 0 {
		return closureRecheck{Verdict: closureRecheckIncomplete, Reason: strings.Join(incomplete, "; "), Bound: max(bound, sourceBound), Unconfirmed: unconfirmed, Skipped: gone, SourceNotes: sourceNotes, DriftNotes: driftNotes}
	}
	if len(sourceNotes) > 0 {
		return closureRecheck{Verdict: closureRecheckIncomplete, Reason: strings.Join(sourceNotes, "; "), Bound: sourceBound, Skipped: gone, SourceNotes: sourceNotes, SourceOnly: true, DriftNotes: driftNotes}
	}
	confirmed.Skipped = gone
	confirmed.DriftNotes = driftNotes
	return confirmed
}

// closureDriftNotes compares the accepted source with the live namespace for a
// closure. driftNotes lists objects that still differ. sourceNotes lists checks
// that were not available, and sourceBound is how long a closure may wait for
// them. Neither is a verdict on health.
func (c *Controller) closureDriftNotes(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) (driftNotes, sourceNotes []string, sourceBound time.Duration) {
	c.mu.Lock()
	inventoryAvailable := c.drift == nil || !c.drift.Inventory || c.sourceInventoryAvailableLocked()
	if inventoryAvailable {
		_, remaining := c.verifySourceInventoryLocked(closure.Result)
		for _, finding := range remaining {
			driftNotes = append(driftNotes, driftNote(finding))
		}
	}
	c.mu.Unlock()
	if !inventoryAvailable {
		// After a restart the inventory is rebuilt from accepted source on the
		// next pass. Not loaded yet is not a contradiction of the repair.
		sourceNotes = append(sourceNotes, "accepted source inventory is loading")
		sourceBound = closureInventoryLoadBound
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
		if err != nil {
			// A source that cannot be rendered or compared is not a
			// contradiction; only a comparison that shows the field still
			// differs is.
			sourceNotes = append(sourceNotes, fmt.Sprintf("source fields for %s/%s were not compared: %v", finding.PrimaryResource.Kind, finding.PrimaryResource.Name, err))
			sourceBound = closureInventoryLoadBound
		} else if !restored {
			driftNotes = append(driftNotes, driftNote(finding))
		}
	}
	return driftNotes, sourceNotes, sourceBound
}

// observerDenied separates a permanent scope denial from an observer that is
// only unavailable: RBAC refusals, and watched dependencies that are
// undeclared, forbidden or unsupported. A timeout, throttle or server error is
// unavailable whatever the detector watches, so it waits within the bound.
func observerDenied(detector sdk.Detector, snapshot sdk.DetectionContext, err error) bool {
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		return true
	}
	if observerTransient(err) {
		return false
	}
	source, scoped := snapshot.(sdk.ScopedObservationSource)
	for _, watch := range detector.Spec().Watches {
		cluster := watch.Kind == "MutatingWebhookConfiguration" || watch.Kind == "ValidatingWebhookConfiguration"
		if !cluster && (watch.Namespace == "" || watch.Namespace == snapshot.Namespace()) {
			continue
		}
		declared := false
		if scoped {
			for _, status := range source.ObservationStatuses() {
				if status.Target.APIVersion != watch.APIVersion || status.Target.Kind != watch.Kind || status.Target.Namespace != watch.Namespace {
					continue
				}
				declared = true
				if status.State == "forbidden" || status.State == "unsupported" {
					return true
				}
			}
		}
		if !declared {
			return true
		}
	}
	return false
}

// observerTransient reports an error that a later observation can clear.
func observerTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return true
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsUnexpectedServerError(err) {
		return true
	}
	var status apierrors.APIStatus
	return errors.As(err, &status) && status.Status().Code >= 500
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
			goto reflections
		}
	}

reflections:
	for {
		select {
		case completion := <-c.reflectionResults:
			c.handleReflectionCompletion(completion)
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
	c.reflections.rescan = true
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
