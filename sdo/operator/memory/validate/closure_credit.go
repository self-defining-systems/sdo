package validate

import (
	"strconv"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// ApplicationOwnsRepair reports whether a repair receipt is one of the plan
// application's own. Closure credit binds an application to the writes it made
// with this rule; a playbook is credited only when its application was the final
// write before Recovered.
//
// It matches the controller's receipt identities, including per-script receipts
// and duplicate suffixes. A bare program-plan kind never proves playbook
// ownership: direct hand repairs also run through the ordinary plan executor.
func ApplicationOwnsRepair(app *contracts.PlanApplication, a *contracts.RepairActionReceipt) bool {
	id := app.GetApplicationId()
	if id == "" {
		return false
	}
	switch a.GetKind() {
	case "sdo-apply", "sdo-mitigate":
		return a.GetActionId() == id
	case "mitigation-menu":
		// The responder menu numbers each applied action of one application.
		n, err := strconv.Atoi(strings.TrimPrefix(a.GetActionId(), id+"-a"))
		return strings.HasPrefix(a.GetActionId(), id+"-a") && err == nil && n >= 1
	case "program-plan", "run-script":
		base := "sdo-program-" + id
		if a.GetActionId() == base {
			return true
		}
		suffix, ok := strings.CutPrefix(a.GetActionId(), base+"-")
		if !ok {
			return false
		}
		suffix = strings.TrimPrefix(suffix, "script-")
		for _, part := range strings.Split(suffix, "-") {
			n, err := strconv.Atoi(part)
			if err != nil || n < 1 {
				return false
			}
		}
		return suffix != ""
	}
	return false
}
