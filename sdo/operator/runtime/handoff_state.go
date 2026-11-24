package runtime

import (
	"slices"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// handoffMutationRounds is how many further mutation rounds the responder may
// spend on one implicating finding after an automatic repair.
const handoffMutationRounds = 1

func findingKey(finding sdk.Finding) string {
	return FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
}

// isProbeFinding reports a failing synthetic-scenario finding of a
// Go-installed traffic detector.
func isProbeFinding(finding sdk.Finding) bool {
	return strings.HasPrefix(finding.DetectorID, memory.TrafficDetectorPrefix) && strings.HasPrefix(finding.RuleID, traffic.RuleIDPrefix)
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
// the detectors have not yet confirmed; nil when nothing is pending. A residual
// is a baseline clue (standing before the incident, unchanged, not an opening
// alert) or active: the stage already gave the alert set its clearing horizon
// and the stability window after its writes settled, so nothing left is lag.
func buildHandoffState(state *IncidentState, report *contracts.StageReport, now time.Time) *contracts.HandoffState {
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
		handoff.ObserveOnly = true
		handoff.ObserveUntil = contractTimestamp(now.UTC().Add(DefaultMitigationRolloutTimeout))
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
	report.HandoffState = buildHandoffState(state, report, now)
}

// recordHandoffState records the hand-off state of what the stage leaves to
// another actor; none when nothing is left.
func (stage *DeterministicResponseStage) recordHandoffState(loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, save func(bool) error) error {
	now := stage.recoveryNow
	if now == nil {
		now = loop.clock
	}
	report.HandoffState = nil
	if len(state.Findings) > 0 {
		report.HandoffState = buildHandoffState(state, report, now())
	}
	return save(false)
}
