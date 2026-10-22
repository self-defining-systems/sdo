package runtime

import (
	"maps"
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// verifiedRepeatBindings selects only controller-verified applications or
// equivalent repairs covering every matched binding, repair and confirmed cause.
// It deliberately does not require prior trust: this is how repeats earn it.
func verifiedRepeatBindings(outcome *contracts.OutcomeRecord) map[string][]program.Bindings {
	if outcome.GetClassification() != "success" || outcome.RepairCommit != nil || outcome.GetResponseLoopExhausted() ||
		len(outcome.GetRepairActions()) == 0 || len(outcome.GetConfirmedRootCauses()) == 0 {
		return nil
	}
	for _, signal := range outcome.GetProgramFailureSignals() {
		if signal != "adhoc_fixed_matched_program" {
			return nil
		}
	}
	revisions := programRunRevisions(outcome)
	bindings := map[string][]program.Bindings{}
	for _, application := range outcome.GetPlanApplications() {
		if (application.FinalEquivalenceVerified != nil && !application.GetFinalEquivalenceVerified()) || !application.GetApplied() || application.GetDryRun() || application.GetRolledBack() ||
			application.GetError() != "" || application.GetRollbackError() != "" ||
			!application.GetVerificationCompleted() || !application.GetVerificationPassed() ||
			application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || len(application.GetProgramIds()) == 0 {
			return nil
		}
		for _, id := range application.GetProgramIds() {
			if !boundPostApplyNoMatch(outcome, application, id, revisions[id]) {
				return nil
			}
			bindings[id] = append(bindings[id], appliedPlanBindings(outcome, application, id)...)
		}
	}
	if len(bindings) == 0 {
		return nil
	}
	covered := func(ref sdk.ObjectRef) bool {
		for _, instances := range bindings {
			for _, instance := range instances {
				for _, bound := range instance {
					if sameResource(ref, bound) {
						return true
					}
				}
			}
		}
		return false
	}
	matched := map[string]bool{}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
			continue
		}
		instance := program.Bindings{}
		for _, binding := range run.GetBindings() {
			instance[binding.GetName()] = objectRefState(binding.GetObject())
		}
		if !slices.ContainsFunc(bindings[run.GetProgramId()], func(applied program.Bindings) bool { return maps.Equal(applied, instance) }) {
			return nil
		}
		matched[run.GetProgramId()] = true
	}
	for id := range bindings {
		if !matched[id] {
			return nil
		}
	}
	for _, cause := range outcome.GetConfirmedRootCauses() {
		if !rootCauseCovered(cause, bindings) {
			return nil
		}
	}
	for _, action := range outcome.GetRepairActions() {
		if !action.GetSuccess() {
			return nil
		}
		if repairActionIsApplication(action, outcome.GetPlanApplications()) {
			continue
		}
		if len(action.GetResources()) == 0 {
			return nil
		}
		for _, ref := range action.GetResources() {
			if !covered(objectRefState(ref)) {
				return nil
			}
		}
	}
	for _, event := range outcome.GetDriftEvents() {
		if !covered(objectRefState(event.GetRef())) {
			return nil
		}
	}
	return bindings
}
