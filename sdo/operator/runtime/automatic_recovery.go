package runtime

import (
	"context"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// automaticRecoveryLocked is the controller's only automatic-stage dispatch
// decision. The completed stage report and dispatch state are saved together
// before ordinary dispatch preparation publishes a worker task.
func (c *Controller) automaticRecoveryLocked(now time.Time, report *contracts.StageReport, stabilityOnly bool, check recoveryCheck, rolloutBlockers []sdk.Finding) {
	// A pass can start while recovery observations run with c.mu released.
	// Let it publish before selecting and freezing a handoff.
	if c.midStage.running || c.pendingLearnedPassReadyLocked() {
		return
	}
	observedAt := laterObservationTime(now, c.clock())
	request := c.currentIncidentRequest
	applied := playbookWrote(report)
	refused := stagePlanRefused(report)
	scoped := applied || refused
	deadline := c.automaticDeadlineLocked(report)
	if applied && report.RecoveryDeadline == nil {
		report.RecoveryDeadline = contractTimestamp(deadline)
	}
	// A failed script has nothing to wait for: the agent takes over now. Only
	// its responder can verify a page, so a page never waits either.
	failed := report.StabilityDeadline == nil && stageScriptsFailed(report) && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages)
	page := pageOpened(request)
	canDispatch := c.codePath != nil && slices.Contains(c.codePath.Selection.ResponseStages, "agent") &&
		(request.ResponseDeadline.IsZero() || observedAt.Before(request.ResponseDeadline))
	// Nor do the writes have anything left to do for a condition they cannot clear.
	beyond := canDispatch && report.StabilityDeadline == nil && agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) &&
		c.conditionBeyondStageLocked(now, *request, check.beyondStage)
	if !failed && !beyond && !page && (applied || refused && report.StabilityDeadline != nil) && observedAt.Before(deadline) && !stageEscalatedIn(request.PriorStages) {
		return
	}
	if stabilityOnly && scoped {
		if report.StabilityDeadline == nil {
			interval := time.Second
			for _, id := range c.recoveryDetectorIDs(*request) {
				interval = max(interval, c.detectorSpecs[id].Interval)
			}
			report.StabilityDeadline = contractTimestamp(c.closureStableSince.Add(c.config.ClosureStabilityWindow + interval))
		}
		return
	}

	// A fresh scoped clear can remove an alert from the handoff. Missing or
	// unconfirmed health cannot establish that users recovered.
	keys := alertKeys(request)
	if scoped {
		keys = c.unresolvedAlertKeysLocked(now, *request)
	} else if !stageEscalatedIn(request.PriorStages) {
		keys = findingKeys(request.Findings)
	}
	if scoped && len(keys) == 0 && report.StabilityDeadline == nil && !page {
		interval := time.Second
		for _, id := range c.recoveryDetectorIDs(*request) {
			interval = max(interval, c.detectorSpecs[id].Interval)
		}
		stabilityDeadline := observedAt.Add(c.config.ClosureStabilityWindow + interval)
		if response := request.ResponseDeadline; !response.IsZero() && response.Before(stabilityDeadline) {
			stabilityDeadline = response
		}
		report.StabilityDeadline = contractTimestamp(stabilityDeadline)
		return
	}
	if applied {
		exit := contracts.RecoveryWait_EXIT_REASON_DEADLINE
		if page || beyond {
			exit = contracts.RecoveryWait_EXIT_REASON_UNCOVERED
		}
		markAutomaticRecovery(report, observedAt, exit)
		markUnsettledRollouts(report, check.record, rolloutBlockers)
	}
	if !canDispatch || stageEscalatedIn(request.PriorStages) || !agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) || scoped && len(keys) == 0 && !page {
		c.escalateLocked(observedAt)
		return
	}

	report.HandoffSelected = true
	report.HandoffKeys = slices.Clone(keys)
	if applied {
		report.HandoffState = buildHandoffState(*request, report)
	}
	c.responderDone = false
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	c.resetDispatchRetryLocked()
}

// handOffUnappliedStageLocked dispatches the agent as soon as a stage that
// wrote nothing ends. Its decision reads the frozen request alone, and once the
// pipeline ends only evidence already queued can start another automatic pass,
// so a further observation could change neither.
func (c *Controller) handOffUnappliedStageLocked(now time.Time) {
	report := c.awaitingAutomaticRecoveryLocked()
	if report == nil || c.config.ExternalIncidentsOnly || playbookWrote(report) || stagePlanRefused(report) {
		return
	}
	c.automaticRecoveryLocked(now, report, false, recoveryCheck{}, nil)
}

// alertsBeyondStage are the alert set's Kubernetes conditions that the settled
// writes of the automatic stage cannot clear. Such a condition reports an
// object as it is now, so it does not lag a repair, unlike a request or
// user-test alert; no applied program targeted it; and its component does not
// depend, over the accepted map, on any component the stage wrote. When any of
// that cannot be established there is none, and the stage waits its bound.
func alertsBeyondStage(view sdk.DetectionContext, m program.AppMap, request IncidentRequest, report *contracts.StageReport) []string {
	written, targeted := map[string]bool{}, map[string]bool{}
	for _, attempt := range report.GetMitigations() {
		receipt := attempt.GetApplyReceipt()
		if !stageReceiptWrote(receipt) {
			continue
		}
		// A script's writes are not all known, nor is the reach of a write to
		// an object the map does not place.
		if len(attempt.GetTouchedObjects()) == 0 || slices.ContainsFunc(receipt.GetPlan().GetActions(), func(a *contracts.Action) bool { return a.GetRunScript() != nil }) {
			return nil
		}
		for _, ref := range attempt.GetTouchedObjects() {
			component, ok := m.ComponentOf(view, objectRefState(ref))
			if !ok {
				return nil
			}
			written[component.Name] = true
		}
		for _, fp := range append(slices.Clone(attempt.GetClearedFindingFingerprints()), attempt.GetRemainingFindingFingerprints()...) {
			targeted[fp] = true
		}
	}
	var beyond []string
	for _, finding := range newRecoveryScope(alertKeys(&request)).blocking(request.Findings) {
		if !slices.Contains(memory.KubernetesConditionDetectorIDs(), finding.DetectorID) || targeted[FindingFingerprint(finding)] {
			continue
		}
		if component, ok := m.ComponentOf(view, finding.PrimaryResource); ok && len(written) > 0 && !dependsOnAny(m, component.Name, written) {
			beyond = append(beyond, findingKey(finding))
		}
	}
	return beyond
}

// dependsOnAny reports whether the component is, or calls directly or
// indirectly, one of the targets.
func dependsOnAny(m program.AppMap, component string, targets map[string]bool) bool {
	seen := map[string]bool{component: true}
	for queue := []string{component}; len(queue) > 0; queue = queue[1:] {
		if targets[queue[0]] {
			return true
		}
		for _, edge := range m.Edges {
			if edge.From == queue[0] && !seen[edge.To] {
				seen[edge.To] = true
				queue = append(queue, edge.To)
			}
		}
	}
	return false
}

// beyondStageAlerts is alertsBeyondStage once the stage's writes settled and
// had the stability window to show an effect the map does not tell of, such as
// freed capacity.
func (c *Controller) beyondStageAlerts(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, request IncidentRequest, check recoveryCheck) []string {
	report := deterministicStageReport(request.PriorStages)
	if !playbookWrote(report) || !check.record.GetWritesSettled() ||
		now.Before(check.record.GetSince().AsTime().Add(c.config.ClosureStabilityWindow)) {
		return nil
	}
	m, _ := acceptedVerificationMap(ctx, c.gitRead, request)
	return alertsBeyondStage(snapshot, m.Program(), request, report)
}

// conditionBeyondStageLocked reports one of the keys firing in this
// observation's own evaluation; a missing or older one starts no agent.
func (c *Controller) conditionBeyondStageLocked(now time.Time, request IncidentRequest, keys []string) bool {
	if len(keys) == 0 {
		return false
	}
	_, _, firing := c.sampledAlertsLocked(now, request)
	return slices.ContainsFunc(keys, func(key string) bool { return firing[key] })
}

func (c *Controller) unresolvedAlertKeysLocked(now time.Time, request IncidentRequest) []string {
	clear, sampledFiring, firing := c.sampledAlertsLocked(now, request)
	var unresolved []string
	for _, key := range alertKeys(&request) {
		id, _, _ := strings.Cut(key, "\x00")
		if firing[key] || !clear[id] && !sampledFiring[id] {
			unresolved = append(unresolved, key)
		}
	}
	return unresolved
}

// sampledAlertsLocked reads this observation's evaluations: the detectors that
// are clear, those that fire, and the keys that fire.
func (c *Controller) sampledAlertsLocked(now time.Time, request IncidentRequest) (clear, sampledFiring, firing map[string]bool) {
	scope := newRecoveryScope(alertKeys(&request))
	clear, sampledFiring, firing = map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, id := range c.recoveryDetectorIDs(request) {
		if !c.recoverySamples[id].matches(now, scope) {
			continue
		}
		states, _ := c.latestEvaluations([]string{id}, now, false)
		if len(states) == 0 || !states[0].EvaluatedAt.Equal(now) {
			continue
		}
		switch states[0].Status {
		case DetectorEvaluationClear:
			clear[id] = true
		case DetectorEvaluationFiring:
			sampledFiring[id] = true
			for _, fp := range states[0].Fingerprints {
				firing[FindingStateKey(id, fp)] = true
			}
		}
	}
	return clear, sampledFiring, firing
}

func (c *Controller) automaticDeadlineLocked(report *contracts.StageReport) time.Time {
	if at := report.GetStabilityDeadline(); at.IsValid() {
		return at.AsTime()
	}
	if at := report.GetRecoveryDeadline(); at.IsValid() {
		return at.AsTime()
	}
	// Legacy completed reports did not carry the stage's internal deadline.
	// Anchor the fallback at the original receipt, without a fresh budget.
	var wait time.Duration
	if c.config.AfterPlaybook == settings.AfterPlaybookWait {
		wait = c.config.AfterPlaybookWait
	}
	bound := automaticRecoveryDeadline(*c.currentIncidentRequest, report, lastStageReceipt(report), nil, nil, wait, c.config.ClosureStabilityWindow)
	if bound.IsZero() {
		return c.responderCompletedAt
	}
	return bound
}

func markAutomaticRecovery(report *contracts.StageReport, now time.Time, exit contracts.RecoveryWait_ExitReason) {
	if report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_DEADLINE {
		return
	}
	completed := report.GetApplyCompletedAt()
	if completed == nil {
		completed = contractTimestamp(lastStageReceipt(report))
	}
	report.RecoveryWait = &contracts.RecoveryWait{ExitReason: exit, Duration: durationpb.New(max(0, now.Sub(completed.AsTime())))}
}

func markUnsettledRollouts(report *contracts.StageReport, recovery *contracts.RecoveryVerification, blockers []sdk.Finding) {
	for _, attempt := range report.GetMitigations() {
		id := attempt.GetApplyReceipt().GetApplicationId()
		attempt.RolloutUnsettledAtDeadline = recovery == nil && attempt.GetApplyReceipt().GetRolloutPending() != nil ||
			slices.ContainsFunc(recovery.GetWrites(), func(write *contracts.WriteSettlement) bool {
				return id != "" && write.GetApplicationId() == id && !write.GetSettled()
			})
		for _, ref := range attempt.GetTouchedObjects() {
			if slices.ContainsFunc(blockers, func(f sdk.Finding) bool { return f.PrimaryResource == objectRefState(ref) }) {
				attempt.RolloutUnsettledAtDeadline = true
			}
		}
	}
}

// stageDispatchRequest narrows only the prepared task. The controller's
// request retains the full opening and joined alert descriptions for closure.
func stageDispatchRequest(request IncidentRequest) IncidentRequest {
	report := deterministicStageReport(request.PriorStages)
	if !report.GetHandoffSelected() || pageOpened(&request) {
		return request
	}
	findings := make([]sdk.Finding, 0, len(report.GetHandoffKeys()))
	for _, finding := range request.Findings {
		if slices.Contains(report.GetHandoffKeys(), findingKey(finding)) {
			findings = append(findings, finding)
		}
	}
	request.Findings = findings
	return request
}

func (c *Controller) awaitingAutomaticRecoveryLocked() *contracts.StageReport {
	if !c.incidentOpen || c.currentIncidentRequest == nil || !c.responderDone || !c.escalatedAt.IsZero() || c.dispatchError != "" {
		return nil
	}
	report := deterministicStageReport(c.currentIncidentRequest.PriorStages)
	if report.GetHandoffSelected() || !stageOnlyClosure(IncidentClosure{Request: *c.currentIncidentRequest, Result: c.currentIncidentResult}) {
		return nil
	}
	return report
}

// An observation already in progress may supply the first clear sample at
// handoff. Use ordinary read budgets and the response deadline, leaving the
// automatic deadline to govern writes and the decision after observation.
func (c *Controller) automaticObservationContext(ctx context.Context, now time.Time) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	report := c.awaitingAutomaticRecoveryLocked()
	if !playbookWrote(report) {
		c.mu.Unlock()
		return ctx, func() {}
	}
	response := c.currentIncidentRequest.ResponseDeadline
	c.mu.Unlock()
	if response.IsZero() {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, response.Sub(laterObservationTime(now, c.clock())))
}

func (c *Controller) loseAutomaticObservation(now time.Time) {
	c.resetEarlyStability()
	c.mu.Lock()
	defer c.mu.Unlock()
	if report := c.awaitingAutomaticRecoveryLocked(); (playbookWrote(report) || stagePlanRefused(report)) && !c.midStage.running && !c.closurePreparingLocked() {
		c.automaticRecoveryLocked(now, report, false, recoveryCheck{}, nil)
	}
}

// A controller restart restores the already-written application's journal on
// demand. The controller state is the deadline authority; the journal is used
// to verify effects and never resumes an application here.
func (c *Controller) restoreAutomaticLoop(ctx context.Context, request IncidentRequest) (*ResponderLoop, error) {
	source, ok := c.dispatcher.(interface{ DeterministicResponseStage() ResponseStage })
	if !ok {
		return nil, nil
	}
	stage, ok := source.DeterministicResponseStage().(*DeterministicResponseStage)
	if !ok || stage.Restore == nil || stage.NewLoop == nil {
		return nil, nil
	}
	loop := stage.NewLoop(request)
	if loop == nil {
		return nil, nil
	}
	if err := stage.Restore(ctx, request, loop); err != nil {
		c.mu.Lock()
		if c.responderLoop == loop {
			c.responderLoop = nil
		}
		c.mu.Unlock()
		loop.CloseWorkingPrograms()
		return nil, err
	}
	c.mu.Lock()
	if c.currentIncidentRequest != nil && c.currentIncidentRequest.IncidentID == request.IncidentID {
		c.responderLoop = loop
	}
	c.mu.Unlock()
	return loop, nil
}

// The step time keys samples, while elapsed observation time counts against the
// response budget. An injected clock may lag the supplied step time.
func laterObservationTime(step, observed time.Time) time.Time {
	if observed.After(step) {
		return observed
	}
	return step
}
