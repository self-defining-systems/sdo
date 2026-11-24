package replay

import "sdo.dev/operator/sdk/program"

func scriptAction(action program.Action) (program.RunScript, bool) {
	switch a := action.(type) {
	case program.RunScript:
		return a, true
	case *program.RunScript:
		if a != nil {
			return *a, true
		}
	}
	return program.RunScript{}, false
}
func containsScript(plan program.Plan) bool {
	for _, a := range plan.Actions {
		if _, ok := scriptAction(a); ok {
			return true
		}
	}
	return false
}
