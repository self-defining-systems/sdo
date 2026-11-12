package runtime

import (
	"context"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// handoffMutationRounds is how many further mutation rounds the responder may
// spend on one implicating finding after an automatic repair.
const handoffMutationRounds = 1

// lagTrend is what the controller saw of a probe finding's SLO error rate while
// it observed the repair settle.
type lagTrend int

const (
	trendUnobserved lagTrend = iota
	trendNonIncreasing
	trendIncreasing
)

func findingKey(finding sdk.Finding) string {
	return FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
}

// isProbeFinding reports a failing synthetic-scenario finding of a
// Go-installed traffic detector: the only findings that can lag a repair.
func isProbeFinding(finding sdk.Finding) bool {
	return strings.HasPrefix(finding.DetectorID, memory.TrafficDetectorPrefix) && strings.HasPrefix(finding.RuleID, traffic.RuleIDPrefix)
}

func probeErrorRate(finding sdk.Finding) (float64, bool) {
	rate, ok := finding.Metadata["error_rate"].(float64)
	return rate, ok
}

// appliedRepairs are the stage attempts that wrote and were not undone.
func appliedRepairs(report *contracts.StageReport) []*contracts.StageMitigation {
	var applied []*contracts.StageMitigation
	for _, attempt := range report.GetMitigations() {
		if receipt := attempt.GetApplyReceipt(); receipt.GetApplied() && !receipt.GetRolledBack() {
			applied = append(applied, attempt)
		}
	}
	return applied
}

func postApplyDiagnose(applied []*contracts.StageMitigation) contracts.HandoffState_PostApplyDiagnose {
	result := contracts.HandoffState_POST_APPLY_DIAGNOSE_NO_MATCH
	for _, attempt := range applied {
		switch attempt.GetPostApplyDiagnose().GetVerdict().GetResult() {
		case contracts.Verdict_RESULT_MATCH:
			return contracts.HandoffState_POST_APPLY_DIAGNOSE_MATCH
		case contracts.Verdict_RESULT_NO_MATCH:
		default:
			result = contracts.HandoffState_POST_APPLY_DIAGNOSE_NOT_RUN
		}
	}
	return result
}

// buildHandoffState is the controller's statement of a repair it applied that
// the detectors have not yet confirmed; nil when nothing is pending. window is
// the observation the controller spent (and, on the responder side, the bound of
// observe_only). trends carry what the settlement extension saw of each probe.
func buildHandoffState(state *IncidentState, report *contracts.StageReport, trends map[string]lagTrend, now time.Time, window time.Duration) *contracts.HandoffState {
	applied := appliedRepairs(report)
	if len(applied) == 0 || len(state.Findings) == 0 {
		return nil
	}
	handoff := &contracts.HandoffState{
		RepairState:       contracts.HandoffState_REPAIR_STATE_PENDING_CONFIRMATION,
		PostApplyDiagnose: postApplyDiagnose(applied),
	}
	for _, attempt := range applied {
		handoff.AppliedPlaybookIds = append(handoff.AppliedPlaybookIds, attempt.GetPlaybookId())
		if at := attempt.GetApplyReceipt().GetCompletedAt(); at != nil && (handoff.AppliedAt == nil || at.AsTime().After(handoff.AppliedAt.AsTime())) {
			handoff.AppliedAt = at
		}
	}
	opening := state.Request.OpeningKeys
	standing := state.Request.StandingKeys
	atOpen := map[string]sdk.Finding{}
	for _, finding := range state.Request.Findings {
		atOpen[findingKey(finding)] = finding
	}
	noMatch := handoff.PostApplyDiagnose == contracts.HandoffState_POST_APPLY_DIAGNOSE_NO_MATCH
	observeOnly := noMatch
	for _, finding := range state.Findings {
		key := findingKey(finding)
		residual := &contracts.HandoffState_ResidualFinding{Key: key, DetectorId: finding.DetectorID}
		if before, ok := atOpen[key]; ok {
			residual.ChangedSinceOpen = before.Severity != finding.Severity || before.Summary != finding.Summary
		}
		switch {
		case noMatch && isProbeFinding(finding) && trends[key] == trendNonIncreasing:
			residual.ResidualClass = contracts.HandoffState_RESIDUAL_CLASS_PROBE_LAG
		case slices.Contains(standing, key) && !slices.Contains(opening, key) && !residual.ChangedSinceOpen:
			residual.ResidualClass = contracts.HandoffState_RESIDUAL_CLASS_BASELINE_CLUE
			if !state.Request.DetectedAt.IsZero() {
				residual.FirstSeen = contractTimestamp(state.Request.DetectedAt.UTC())
			}
		default:
			residual.ResidualClass = contracts.HandoffState_RESIDUAL_CLASS_ACTIVE
			observeOnly = false
		}
		handoff.ResidualFindings = append(handoff.ResidualFindings, residual)
	}
	if observeOnly {
		bound := window
		if bound <= 0 {
			bound = DefaultMitigationRolloutTimeout
		}
		handoff.ObserveOnly = true
		handoff.ObserveUntil = contractTimestamp(now.UTC().Add(bound))
	} else {
		handoff.MutationRoundsPerFinding = handoffMutationRounds
	}
	return handoff
}

// attachHandoffState gives the deterministic report its state when the stage
// did not build one (a resumed stage). An existing state is never rewritten.
func attachHandoffState(state *IncidentState, now time.Time) {
	report := deterministicStageReport(state.PriorStages)
	if report == nil || report.HandoffState != nil {
		return
	}
	report.HandoffState = buildHandoffState(state, report, nil, now, 0)
}

// lagSettlementApplies: the repair was applied, every applied program no longer
// matches, the recovery bound ended the wait, and every residual finding is a
// probe or a standing clue. Anything else needs another actor now.
func lagSettlementApplies(state *IncidentState, report *contracts.StageReport) bool {
	applied := appliedRepairs(report)
	if len(applied) == 0 || len(state.Findings) == 0 || postApplyDiagnose(applied) != contracts.HandoffState_POST_APPLY_DIAGNOSE_NO_MATCH {
		return false
	}
	if report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_DEADLINE {
		return false
	}
	probes := false
	for _, finding := range state.Findings {
		switch {
		case isProbeFinding(finding):
			probes = true
		case slices.Contains(state.Request.StandingKeys, findingKey(finding)) && !slices.Contains(state.Request.OpeningKeys, findingKey(finding)):
		default:
			return false
		}
	}
	return probes
}

// settleLagBeforeHandoff extends the recovery wait to one detector window when
// the repair is applied and only probe lag remains, then records the hand-off
// state. The residual error rate must not rise while it waits: a rising rate is
// a fix that did not take, so the extension ends and the responder gets it.
func (stage *DeterministicResponseStage) settleLagBeforeHandoff(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, save func(bool) error) error {
	now := stage.recoveryNow
	if now == nil {
		now = loop.clock
	}
	trends := map[string]lagTrend{}
	var extended time.Duration
	var window time.Duration
	if lagSettlementApplies(state, report) {
		var err error
		window, extended, err = stage.extendLagSettlement(ctx, loop, state, report, trends, now)
		if err != nil {
			return err
		}
	}
	if len(state.Findings) == 0 {
		report.HandoffState = nil
		return save(false)
	}
	report.HandoffState = buildHandoffState(state, report, trends, now(), window)
	if report.HandoffState != nil && extended > 0 {
		report.HandoffState.SettlementExtension = durationpb.New(extended)
	}
	return save(false)
}

func (stage *DeterministicResponseStage) extendLagSettlement(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, trends map[string]lagTrend, now func() time.Time) (window, extended time.Duration, err error) {
	view, err := loop.snapshotView(ctx)
	if err != nil {
		return 0, 0, nil
	}
	interval := stage.ObservationInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	for _, finding := range state.Findings {
		for _, detector := range loop.Detectors {
			if detector.Spec().ID == finding.DetectorID && isProbeFinding(finding) {
				window = max(window, detectorClearingHorizon(detector, view)+2*detector.Spec().Interval)
			}
		}
	}
	remaining := window
	if check := loop.stageRecoveryCheck(ctx, view, state); check.record.GetWritesSettled() && check.record.GetSince() != nil {
		remaining = window - now().Sub(check.record.GetSince().AsTime())
	}
	if deadline := state.Request.ResponseDeadline; !deadline.IsZero() {
		remaining = min(remaining, deadline.Sub(now()))
	}
	if remaining <= 0 {
		return window, 0, nil
	}
	wait := stage.recoveryWait
	if wait == nil {
		wait = func(ctx context.Context, d time.Duration) error {
			timer := lifecycleio.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	rates := map[string]float64{}
	for _, finding := range state.Findings {
		if rate, ok := probeErrorRate(finding); ok {
			rates[findingKey(finding)] = rate
		}
	}
	started := now()
	deadline := started.Add(remaining)
	defer func() { extended = now().Sub(started) }()
	for now().Before(deadline) {
		if err := wait(ctx, min(interval, deadline.Sub(now()))); err != nil {
			return window, 0, err
		}
		response := ToolResponse{}
		loop.verify(ctx, nil, &response, true)
		updateStageFindings(state, response)
		_, stages := loop.settleRecoveryApplicationsLocked(ctx, IncidentResult{PlanApplications: loop.applicationEvidenceLocked()}, []*contracts.StageReport{report})
		if len(stages) > 0 {
			proto.Reset(report)
			proto.Merge(report, stages[0])
		}
		if len(state.Findings) == 0 {
			// Silence is not recovery: keep observing until the same proof the
			// stage's own wait requires holds, or the window ends.
			if stageRecoveredNow(ctx, loop, state) {
				break
			}
			continue
		}
		stop := false
		for _, finding := range state.Findings {
			key := findingKey(finding)
			rate, ok := probeErrorRate(finding)
			previous, seen := rates[key]
			switch {
			case !isProbeFinding(finding) || !ok || !seen:
				// Not lag, or a finding that arose while observing: the
				// responder gets it with the ordinary mandate.
				trends[key] = trendUnobserved
				stop = stop || !slices.Contains(state.Request.StandingKeys, key)
			case rate > previous:
				trends[key] = trendIncreasing
				stop = true
			default:
				if trends[key] != trendIncreasing {
					trends[key] = trendNonIncreasing
				}
				rates[key] = rate
			}
		}
		if stop {
			return window, 0, nil
		}
	}
	if len(state.Findings) == 0 {
		if stageRecoveredNow(ctx, loop, state) {
			report.RecoveryWait = &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_CLEARED, Duration: report.GetRecoveryWait().GetDuration()}
			var settling []*contracts.StageMitigation
			for _, attempt := range report.Mitigations {
				if attempt.Result == "applied_still_matching" && writeVerifiedForSettling(attempt) {
					settling = append(settling, attempt)
				}
			}
			promoteSettledAttempts(settling)
		}
	}
	return window, 0, nil
}

func stageRecoveredNow(ctx context.Context, loop *ResponderLoop, state *IncidentState) bool {
	view, err := loop.snapshotView(ctx)
	return err == nil && decideStageRecovery(loop.stageRecoveryCheck(ctx, view, state), "") == stageCleared
}
