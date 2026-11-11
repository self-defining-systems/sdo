package program_test

import (
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestRestartRolloutAcceptsEveryRollingWorkload(t *testing.T) {
	for kind, ok := range map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "ReplicaSet": false, "Pod": false} {
		apiVersion := "apps/v1"
		if kind == "Pod" {
			apiVersion = "v1"
		}
		ref := sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: "app", Name: "w"}
		err := (program.Plan{Actions: []program.Action{program.RestartRollout{Ref: ref, Reason: "restart"}}}).Validate()
		if (err == nil) != ok {
			t.Errorf("RestartRollout of a %s: %v, want ok=%v", kind, err, ok)
		}
	}
}
