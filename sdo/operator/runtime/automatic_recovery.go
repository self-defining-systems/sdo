package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// automaticRecoveryLocked decides whether the shared recovery proof can close
// the incident, whether to hand off, and when an unsettled wait must end.
// The stage report and dispatch decision are saved before any worker task.
func (c *Controller) automaticRecoveryLocked(now time.Time, report *contracts.StageReport, snapshot sdk.DetectionContext, check recoveryCheck, rolloutBlockers []sdk.Finding, verified bool) (closeIncident bool, escalationReason string) {
	observedAt := laterObservationTime(now, c.clock())
	request := c.currentIncidentRequest
	applied := playbookWrote(report)
	scoped := applied || stagePlanRefused(report)
	extension := report.GetStabilityDeadline()
	if extension.IsValid() && observedAt.Before(extension.AsTime()) {
		return false, ""
	}
	if verified {
		if extension.IsValid() {
			report.StabilityDeadlineEvaluated = true
		}
		return true, ""
	}

	keys := alertKeys(request)
	if scoped {
		deadline := c.automaticApplyDeadlineLocked(report)
		if applied && report.RecoveryDeadline == nil {
			report.RecoveryDeadline = contractTimestamp(deadline)
		}
		if observedAt.Before(deadline) {
			return false, ""
		}

		if !report.GetStabilityDeadlineEvaluated() {
			firing, unavailable := c.automaticAlertStatusLocked(now, *request, keys, snapshot)
			if len(firing) == 0 && !extension.IsValid() {
				end := c.automaticVerificationExtensionLocked(observedAt, *request)
				report.StabilityDeadline = contractTimestamp(end)
				if observedAt.Before(end) {
					return false, ""
				}
			}
			if report.GetStabilityDeadline().IsValid() {
				report.StabilityDeadlineEvaluated = true
			}
			keys = append(firing, unavailable...)
			slices.Sort(keys)
		} else {
			// A clear decision consumed the handoff opportunity. Continue to
			// observe Recovered without reopening it on a subsequent read.
			keys = nil
		}
		if len(keys) == 0 {
			if observedAt.Before(c.automaticResponseDeadlineLocked()) {
				return false, ""
			}
			// The extension resolved handoff. Closing still requires current
			// evidence that every alert cleared.
			firing, unavailable := c.automaticAlertStatusLocked(now, *request, alertKeys(request), snapshot)
			clear := len(firing) == 0 && len(unavailable) == 0
			if clear && !c.closureStableSince.IsZero() {
				return false, ""
			}

			reason := "recovery unconfirmed at response deadline"
			switch {
			case !clear:
				reason = fmt.Sprintf("alerts unresolved at response deadline (firing=%q, unavailable=%q)", firing, unavailable)
			case len(rolloutBlockers) != 0 || check.record != nil && !check.record.GetWritesSettled():
				reason = "write never settled"
			}
			if report.Error != "" {
				report.Error += ". "
			}
			report.Error += reason
			markAutomaticRecovery(report, observedAt, contracts.RecoveryWait_EXIT_REASON_DEADLINE)
			markUnsettledRollouts(report, check.record, rolloutBlockers)
			c.escalateLocked(observedAt)
			if !clear {
				return false, ""
			}
			return true, reason
		}
	} else if !stageEscalatedIn(request.PriorStages) {
		keys = findingKeys(request.Findings)
	}
	if applied {
		markAutomaticRecovery(report, observedAt, contracts.RecoveryWait_EXIT_REASON_DEADLINE)
		markUnsettledRollouts(report, check.record, rolloutBlockers)
	}
	canDispatch := c.codePath != nil && slices.Contains(c.codePath.Selection.ResponseStages, "agent") &&
		(request.ResponseDeadline.IsZero() || observedAt.Before(request.ResponseDeadline))
	if !canDispatch || stageEscalatedIn(request.PriorStages) || !agentMayFollowPlaybook(c.config.AfterPlaybook, request.PriorStages) {
		c.escalateLocked(observedAt)
		return false, ""
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
	return false, ""
}

// A scoped firing sample identifies the failing keys. A pending, errored
// or absent sample cannot establish either failure or clearance.
func (c *Controller) automaticAlertStatusLocked(now time.Time, request IncidentRequest, keys []string, snapshot sdk.DetectionContext) (firing, unavailable []string) {
	if snapshot == nil {
		return nil, keys
	}
	scope := newRecoveryScope(keys)
	observed := make(map[string]DetectorEvaluationStatus)
	failed := make(map[string]bool)
	for _, id := range c.recoveryDetectorIDs(request) {
		if !c.recoverySamples[id].matches(now, scope) {
			continue
		}
		states, _ := c.latestEvaluations([]string{id}, now, false)
		if len(states) == 0 || !states[0].EvaluatedAt.Equal(now) {
			continue
		}
		observed[id] = states[0].Status
		if states[0].Status == DetectorEvaluationFiring {
			for _, fp := range states[0].Fingerprints {
				failed[FindingStateKey(id, fp)] = true
			}
		}
	}
	for _, key := range keys {
		id, _, _ := strings.Cut(key, "\x00")
		if failed[key] {
			firing = append(firing, key)
			continue
		}
		status := observed[id]
		if status == DetectorEvaluationClear {
			continue
		}
		// A detector can read one scenario while a sibling is unavailable.
		if (status == DetectorEvaluationPending || status == DetectorEvaluationFiring) &&
			detectorConfirmedIn(c.detectors[id], snapshot, recoveryScope{key: {}}) {
			continue
		}
		unavailable = append(unavailable, key)
	}
	return firing, unavailable
}

func (c *Controller) automaticVerificationExtensionLocked(now time.Time, request IncidentRequest) time.Time {
	var interval time.Duration
	for _, id := range c.recoveryDetectorIDs(request) {
		interval = max(interval, c.detectorSpecs[id].Interval)
	}
	if interval == 0 {
		interval = time.Second
	}
	end := now.Add(interval + c.config.ClosureStabilityWindow)
	if cap := request.ResponseDeadline; !cap.IsZero() && cap.Before(end) {
		end = cap
	}
	return end
}

func (c *Controller) automaticDeadlineLocked(report *contracts.StageReport) time.Time {
	if report.GetStabilityDeadlineEvaluated() {
		return c.automaticResponseDeadlineLocked()
	}
	if at := report.GetStabilityDeadline(); at.IsValid() {
		return at.AsTime()
	}
	return c.automaticApplyDeadlineLocked(report)
}

func (c *Controller) automaticResponseDeadlineLocked() time.Time {
	if at := c.currentIncidentRequest.ResponseDeadline; !at.IsZero() {
		return at
	}
	return c.recoveryGraceEndsLocked()
}

func (c *Controller) automaticApplyDeadlineLocked(report *contracts.StageReport) time.Time {
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
	if !playbookWrote(report) && !stagePlanRefused(report) {
		c.mu.Unlock()
		return ctx, func() {}
	}
	response := c.currentIncidentRequest.ResponseDeadline
	c.mu.Unlock()
	remaining := response.Sub(laterObservationTime(now, c.clock()))
	if response.IsZero() || remaining <= 0 {
		// The response deadline caps dispatch. An observation at that bound
		// still has its normal read budget so a clear can enter Recovered.
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, remaining)
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
