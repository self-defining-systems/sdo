package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// playbookWrote reports an attempt of the automatic stage that applied its
// plan, whether or not the stage later reverted it: a learned playbook that
// matched and wrote.
func playbookWrote(report *contracts.StageReport) bool {
	for _, attempt := range report.GetMitigations() {
		if attempt.GetApplyReceipt().GetApplied() {
			return true
		}
	}
	return false
}

func playbookApplied(reports []*contracts.StageReport) bool {
	return playbookWrote(deterministicStageReport(reports))
}

// agentMayFollowPlaybook is the one decision response.after_playbook makes for
// the stage pipeline: once a playbook applied and stayed applied, the agent
// follows only under the agent policy (the unset policy). Without an applied
// playbook the policy has nothing to say.
func agentMayFollowPlaybook(policy string, reports []*contracts.StageReport) bool {
	if !playbookApplied(reports) {
		return true
	}
	return policy == "" || policy == settings.AfterPlaybookAgent
}

// stageHandoffWarranted decides whether an unverified incident whose only
// response was the automatic stage is handed to the agent. Before any applied
// playbook, any firing health finding (or, at the deadline, any active health
// detector) is work. After one, only the opening alerts are: another alert
// never dispatches the agent by itself, and the policy decides for the
// opening ones.
func stageHandoffWarranted(policy string, reports []*contracts.StageReport, scope recoveryScope, findings []sdk.Finding, deadlinePassed, openingActive, healthActive bool) bool {
	if !playbookApplied(reports) {
		return len(findings) > 0 || (deadlinePassed && healthActive)
	}
	if !agentMayFollowPlaybook(policy, reports) {
		return false
	}
	return len(scope.blocking(findings)) > 0 || (deadlinePassed && openingActive)
}
