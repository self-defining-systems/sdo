package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

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

func sameResource(a, b sdk.ObjectRef) bool {
	return a.Kind == b.Kind && a.Namespace == b.Namespace && a.Name == b.Name
}
