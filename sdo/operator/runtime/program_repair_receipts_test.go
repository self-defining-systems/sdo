package runtime

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestProgramApplicationsProduceControllerRepairReceipts(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	started := time.Now().UTC()
	applications := []RecordedPlanApplication{{ID: "application-1", ProgramIDs: []string{"targetport"}, StartedAt: started, CompletedAt: started.Add(time.Second), Result: PlanApplicationResult{Applied: true, ActionsApplied: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8080}}}}}
	receipts := ProgramRepairReceipts(applications, nil)
	if len(receipts) != 1 || !receipts[0].Success || len(receipts[0].Resources) != 1 || receipts[0].Resources[0] != ref || receipts[0].StartedAt != started {
		t.Fatalf("controller receipt: %+v", receipts)
	}
}
