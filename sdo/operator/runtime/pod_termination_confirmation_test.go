package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/program"
)

// A Pod that is gone is a plain applied delete: nothing to flag.
func TestDeletePodGoneIsNotRecordedAsTerminating(t *testing.T) {
	e, _, ref := runtimeActionFixture()
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeletePod{Ref: ref}}}, false)
	if err != nil || result.ActionReports[0].Status != "applied" || result.ActionReports[0].Observed != "" {
		t.Fatalf("gone Pod delete report: %+v %v", result.ActionReports[0], err)
	}
}
