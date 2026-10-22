package runtime

import (
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	// ReflectionDepthFull authors operational memory from the incident.
	ReflectionDepthFull = "full"
	// ReflectionDepthRefine confirms accepted programs that already resolved the
	// incident and edits memory only on contrary evidence.
	ReflectionDepthRefine = "refine"
)

// ReflectionDepth is a pure decision over typed outcome facts. It returns
// refine only when accepted programs, already installed in memory, diagnosed,
// repaired and independently verified every fault: every repair action was a
// successful program action, every plan was applied and verified NoMatch, no
// source repair or program failure signal exists, and every program that
// matched was applied. Anything else needs a full reflection.
func ReflectionDepth(outcome *contracts.OutcomeRecord, installedProgramIDs []string) string {
	if outcome == nil || outcome.GetClassification() != "success" || outcome.RepairCommit != nil ||
		len(outcome.GetProgramFailureSignals()) != 0 || len(outcome.GetRepairActions()) == 0 || len(outcome.GetPlanApplications()) == 0 {
		return ReflectionDepthFull
	}
	for _, action := range outcome.GetRepairActions() {
		if !action.GetSuccess() || (action.GetKind() != "program-plan" && action.GetKind() != "sdo-apply") {
			return ReflectionDepthFull
		}
	}
	applied := map[string]bool{}
	for _, application := range outcome.GetPlanApplications() {
		if !application.GetApplied() || application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || len(application.GetProgramIds()) == 0 {
			return ReflectionDepthFull
		}
		for _, id := range application.GetProgramIds() {
			if !slices.Contains(installedProgramIDs, id) {
				return ReflectionDepthFull
			}
			applied[id] = true
		}
	}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH && !applied[run.GetProgramId()] {
			return ReflectionDepthFull
		}
	}
	return ReflectionDepthRefine
}
