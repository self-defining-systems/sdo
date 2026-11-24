package runtime

import (
	"context"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// automaticRecoveryLocked is the controller's only automatic-stage dispatch
// decision. The completed stage report and dispatch state are saved together
// before ordinary dispatch preparation publishes a worker task.
func (c *Controller) automaticRecoveryLocked(now time.Time, report *contracts.StageReport, stabilityOnly bool, check recoveryCheck, rolloutBlockers []sdk.Finding) {
	observedAt := laterObservationTime(now, c.clock())
	request := c.currentIncidentRequest
	applied := playbookWrote(report)
	refused := stagePlanRefused(report)
	scoped := applied || refused
	deadline := c.automaticDeadlineLocked(report)
	if applied && report.RecoveryDeadline == nil {
		report.RecoveryDeadline = contractTimestamp(deadline)
	}
	if applied && observedAt.Before(deadline) && !stageEscalatedIn(request.PriorStages) {
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
	if applied {
		markAutomaticRecovery(report, observedAt, contracts.RecoveryWait_EXIT_REASON_DEADLINE)
		markUnsettledRollouts(report, check.record, rolloutBlockers)
	}
	canDispatch := c.codePath != nil && slices.Contains(c.codePath.Selection.ResponseStages, "agent") &&
		(request.ResponseDeadline.IsZero() || observedAt.Before(request.ResponseDeadline))
	if !canDispatch || stageEscalatedIn(request.PriorStages) || !agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) || scoped && len(keys) == 0 {
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

func (c *Controller) unresolvedAlertKeysLocked(now time.Time, request IncidentRequest) []string {
	keys := alertKeys(&request)
	scope := newRecoveryScope(keys)
	clear := make(map[string]bool)
	firing := make(map[string]bool)
	sampledFiring := make(map[string]bool)
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
	var unresolved []string
	for _, key := range keys {
		id, _, _ := strings.Cut(key, "\x00")
		if firing[key] || !clear[id] && !sampledFiring[id] {
			unresolved = append(unresolved, key)
		}
	}
	return unresolved
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
	if !report.GetHandoffSelected() {
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
