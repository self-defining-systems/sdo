package runtime

import (
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
)

// The menu is a convenience over the responder's ordinary executor. It adds
// no authority, observation requirements or mutation guards.
func productionResponderMenu(_ IncidentRequest, _ kubernetes.Interface, _ []sdk.Detector, _ *PlanSafetyPolicy, _ *ResponderMenuSafety) *ResponderMenuConfig {
	return &ResponderMenuConfig{Direct: true}
}
