package runtime

import (
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
)

// playbookWrote reports an attempt of the automatic stage that applied its
// live application resources, including writes that landed partially.
func playbookWrote(report *contracts.StageReport) bool {
	for _, attempt := range report.GetMitigations() {
		if stageReceiptWrote(attempt.GetApplyReceipt()) {
			return true
		}
	}
	return false
}

func playbookApplied(reports []*contracts.StageReport) bool {
	return playbookWrote(deterministicStageReport(reports))
}

// agentMayFollowPlaybook applies the configured policy once a playbook wrote.
func agentMayFollowPlaybook(policy string, reports []*contracts.StageReport) bool {
	if !playbookApplied(reports) {
		return true
	}
	return policy == "" || policy == settings.AfterPlaybookAgent
}

func stageReceiptWrote(receipt *contracts.PlanApplication) bool {
	if receipt == nil || receipt.GetDryRun() {
		return false
	}
	for _, effect := range receipt.GetObjectEffects() {
		if effect.GetState() != "not_applied" {
			return true
		}
	}
	uncertain := receipt.GetStartedAt() != nil && receipt.GetCompletedAt() == nil
	for _, report := range receipt.GetActionReports() {
		uncertain = uncertain || report.GetFailureReason() == "outcome_unknown"
	}
	actions := receipt.GetActionsApplied()
	if !uncertain && !receipt.GetApplied() && len(actions) == 0 && receipt.GetRolloutPending() == nil {
		return false
	}
	if len(actions) == 0 || uncertain {
		actions = receipt.GetPlan().GetActions()
	}
	for _, action := range actions {
		switch action.GetKind().(type) {
		case *contracts.Action_WaitFor, *contracts.Action_CheckStoreCredential, *contracts.Action_StoreBackup,
			*contracts.Action_ProposeSourceChange, *contracts.Action_Escalate:
			continue
		default:
			return true
		}
	}
	// Legacy summaries may record a write without its actions.
	return len(actions) == 0
}

// stageScriptsFailed reports that every program that may have written ended in
// a script that exited non-zero, timed out, was killed or was refused. Its writes stay
// unknown evidence; only a program that returned success is worth waiting for.
func stageScriptsFailed(report *contracts.StageReport) bool {
	failed := false
	for _, attempt := range report.GetMitigations() {
		receipt := attempt.GetApplyReceipt()
		if !stageReceiptWrote(receipt) {
			continue
		}
		actions := receipt.GetPlan().GetActions()
		if receipt.GetCompletedAt() == nil || !slices.ContainsFunc(receipt.GetActionReports(), func(action *contracts.ActionReport) bool {
			index := int(action.GetActionIndex())
			return index < len(actions) && actions[index].GetRunScript() != nil && (action.GetStatus() == "unknown" || action.GetFailureReason() == ScriptRefusedReason)
		}) {
			return false
		}
		failed = true
	}
	return failed
}
