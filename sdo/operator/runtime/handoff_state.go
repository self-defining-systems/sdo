package runtime

import (
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
	"strings"
)

// handoffMutationRounds is how many further mutation rounds the responder may
// spend on one implicating finding after an automatic repair.
const handoffMutationRounds = 1

func findingKey(finding sdk.Finding) string {
	return FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
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

// buildHandoffState carries the applied repairs and frozen residual alerts.
func buildHandoffState(request IncidentRequest, report *contracts.StageReport) *contracts.HandoffState {
	applied := appliedRepairs(report)
	if len(applied) == 0 {
		return nil
	}
	handoff := &contracts.HandoffState{
		RepairState:              contracts.HandoffState_REPAIR_STATE_PENDING_CONFIRMATION,
		PostApplyDiagnose:        postApplyDiagnose(applied),
		MutationRoundsPerFinding: handoffMutationRounds,
	}
	for _, attempt := range applied {
		handoff.AppliedPlaybookIds = append(handoff.AppliedPlaybookIds, attempt.GetPlaybookId())
		if at := attempt.GetApplyReceipt().GetCompletedAt(); at != nil && (handoff.AppliedAt == nil || at.AsTime().After(handoff.AppliedAt.AsTime())) {
			handoff.AppliedAt = at
		}
	}
	for _, finding := range request.Findings {
		key := findingKey(finding)
		if slices.Contains(report.GetHandoffKeys(), key) {
			handoff.ResidualFindings = append(handoff.ResidualFindings, &contracts.HandoffState_ResidualFinding{
				Key: key, DetectorId: finding.DetectorID, ResidualClass: contracts.HandoffState_RESIDUAL_CLASS_ACTIVE,
			})
		}
	}
	return handoff
}

func isProbeFinding(finding sdk.Finding) bool {
	return strings.HasPrefix(finding.DetectorID, memory.TrafficDetectorPrefix) && strings.HasPrefix(finding.RuleID, traffic.RuleIDPrefix)
}
