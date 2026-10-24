package runtime

import "sdo.dev/operator/sdk/program"

// planAuthorizationMode records how a plan obtains its value, not a policy grant.
func planAuthorizationMode(action program.Action) string {
	switch a := action.(type) {
	case program.SetField:
		if a.Value.Origin() == program.SourceValue {
			return "accepted-source"
		}
		return string(a.Value.Origin())
	case *program.SetField:
		if a != nil {
			return planAuthorizationMode(*a)
		}
	case program.ReconcileFromSource, *program.ReconcileFromSource, program.CreateFromSource, *program.CreateFromSource:
		return "accepted-source"
	}
	return "incident-namespace"
}
