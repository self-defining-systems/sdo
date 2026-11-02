package runtime

import (
	"time"

	"sdo.dev/operator/sdk"
)

// socialRepeatClosure is handwritten toy evidence with the S42 social-repeat
// shape: reported successful actions, aggregate closure failed, no independent
// per-cause verification. It contains no captured output or harness verdict.
func socialRepeatClosure(workspace IncidentWorkspace) IncidentClosure {
	c := nativeClosure(workspace)
	c.Request.RepairPolicy = "recorded-actions"
	c.Result.ResponderSessionID = "responder-social-repeat"
	c.Result.Status = IncidentFailed
	c.FinalDetectorStates = nil
	c.Request.ResponseDeadline = c.ResponderCompletedAt.Add(-time.Second)
	c.Result.ConfirmedRootCauses = []ConfirmedRootCause{{
		Summary: "unsupported scheduling constraint", Resources: []sdk.ObjectRef{{Kind: "Deployment", Namespace: "default", Name: "toy-user"}},
		Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "responder", Observation: "workload became ready after removing selector"}},
	}, {
		Summary: "admission trust configuration", Resources: []sdk.ObjectRef{{Kind: "Deployment", Namespace: "default", Name: "toy-front"}},
		Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "responder", Observation: "pod creation resumed after trust repair"}},
	}}
	c.Result.RepairActions = []RepairActionReceipt{{ActionID: "recorded-selector-repair", Kind: "kubectl-patch", Target: "Deployment/default/toy-user", Summary: "remove unsupported selector", Details: "command succeeded", Success: true, Resources: c.Result.ConfirmedRootCauses[0].Resources, StartedAt: c.DetectedAt, CompletedAt: c.ResponderCompletedAt}}
	return c
}

