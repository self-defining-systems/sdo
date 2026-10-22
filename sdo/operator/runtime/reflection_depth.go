package runtime

import (
	"maps"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const (
	// ReflectionDepthFull authors operational memory from the incident.
	ReflectionDepthFull = "full"
	// ReflectionDepthRefine confirms accepted programs that already resolved the
	// incident and edits memory only on contrary evidence.
	ReflectionDepthRefine = "refine"
)

// ReflectionDepthInput is everything the depth decision may read.
type ReflectionDepthInput struct {
	Outcome *contracts.OutcomeRecord
	// InstalledProgramIDs are the programs in the accepted memory snapshot.
	InstalledProgramIDs []string
	// SessionMode is the final mode after any fresh fallback. Refine needs the
	// responder's own session, so only "resume" can refine.
	SessionMode string
	// Attempt is the validation attempt; a retry corrects rejected edits.
	Attempt uint32
	// HasDiagnostics reports rejected-edit diagnostics from the broker.
	HasDiagnostics bool
}

// ReflectionDepth is a pure decision over typed controller facts. It returns
// refine only when accepted programs, already installed in memory, diagnosed,
// repaired and independently verified every fault in the responder's resumed
// session: each plan application was applied, not dry-run or rolled back, and
// its post-apply NoMatch is bound to the applied bindings by controller-recorded
// tool evidence; every repair action is one of those applications; every
// matched binding was applied and every confirmed root cause is covered by an
// applied binding; and no source repair, drift event, exhausted loop or program
// failure signal exists. Anything else needs a full reflection.
func ReflectionDepth(input ReflectionDepthInput) string {
	outcome := input.Outcome
	if outcome == nil || input.SessionMode != "resume" || input.Attempt > 1 || input.HasDiagnostics ||
		outcome.GetClassification() != "success" || outcome.RepairCommit != nil ||
		len(outcome.GetProgramFailureSignals()) != 0 || len(outcome.GetDriftEvents()) != 0 || outcome.GetResponseLoopExhausted() ||
		len(outcome.GetRepairActions()) == 0 || len(outcome.GetPlanApplications()) == 0 {
		return ReflectionDepthFull
	}
	revisions := programRunRevisions(outcome)
	appliedBindings := map[string][]program.Bindings{}
	for _, application := range outcome.GetPlanApplications() {
		if !application.GetApplied() || application.GetDryRun() || application.GetRolledBack() || application.GetError() != "" || application.GetRollbackError() != "" ||
			!application.GetVerificationCompleted() || !application.GetVerificationPassed() ||
			application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || len(application.GetProgramIds()) == 0 {
			return ReflectionDepthFull
		}
		for _, id := range application.GetProgramIds() {
			if !slices.Contains(input.InstalledProgramIDs, id) || !boundPostApplyNoMatch(outcome, application, id, revisions[id]) {
				return ReflectionDepthFull
			}
			appliedBindings[id] = append(appliedBindings[id], appliedPlanBindings(outcome, application, id)...)
		}
	}
	for _, action := range outcome.GetRepairActions() {
		if !action.GetSuccess() || !repairActionIsApplication(action, outcome.GetPlanApplications()) {
			return ReflectionDepthFull
		}
	}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
			continue
		}
		bindings := program.Bindings{}
		for _, binding := range run.GetBindings() {
			bindings[binding.GetName()] = objectRefState(binding.GetObject())
		}
		if !slices.ContainsFunc(appliedBindings[run.GetProgramId()], func(applied program.Bindings) bool { return maps.Equal(applied, bindings) }) {
			return ReflectionDepthFull
		}
	}
	for _, cause := range outcome.GetConfirmedRootCauses() {
		if !rootCauseCovered(cause, appliedBindings) {
			return ReflectionDepthFull
		}
	}
	return ReflectionDepthRefine
}

// repairActionIsApplication accepts only receipts that the controller's plan
// applications account for: "sdo-apply" must carry an application ID and
// "program-plan" is the receipt ProgramRepairReceipts derives from one. Any
// other kind, including a relabelled manual action, needs a full reflection.
func repairActionIsApplication(action *contracts.RepairActionReceipt, applications []*contracts.PlanApplication) bool {
	for _, application := range applications {
		switch action.GetKind() {
		case "sdo-apply":
			if action.GetActionId() == application.GetApplicationId() {
				return true
			}
		case "program-plan":
			receipt := "sdo-program-" + application.GetApplicationId()
			suffix, derived := strings.CutPrefix(action.GetActionId(), receipt+"-")
			if application.GetApplicationId() != "" && (action.GetActionId() == receipt || (derived && suffix != "" && strings.Trim(suffix, "0123456789") == "")) {
				return true
			}
		}
	}
	return false
}

// appliedPlanBindings returns the bindings of the plans that the recorded
// apply call for this application selected for the program.
func appliedPlanBindings(outcome *contracts.OutcomeRecord, application *contracts.PlanApplication, id string) []program.Bindings {
	plans := map[string]ToolRequest{}
	var applied []program.Bindings
	for _, call := range outcome.GetProgramToolCalls() {
		request, response, ok := parseProgramToolCall(call)
		if !ok {
			continue
		}
		if call.GetCommand() == "plan" && response.PlanID != "" {
			plans[response.PlanID] = request
			continue
		}
		if call.GetCommand() != "apply" || response.ApplicationID != application.GetApplicationId() {
			continue
		}
		for _, planID := range request.PlanIDs {
			if plan, exists := plans[planID]; exists && plan.ProgramID == id {
				applied = append(applied, plan.Bindings)
			}
		}
	}
	return applied
}

func rootCauseCovered(cause *contracts.ConfirmedRootCause, appliedBindings map[string][]program.Bindings) bool {
	for _, resource := range cause.GetResources() {
		ref := objectRefState(resource)
		for _, plans := range appliedBindings {
			for _, bindings := range plans {
				if slices.ContainsFunc(slices.Collect(maps.Values(bindings)), func(bound sdk.ObjectRef) bool { return sameResource(bound, ref) }) {
					return true
				}
			}
		}
	}
	return false
}

func sameResource(a, b sdk.ObjectRef) bool {
	return a.Kind == b.Kind && a.Namespace == b.Namespace && a.Name == b.Name
}
