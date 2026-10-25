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
	applications := []RecordedPlanApplication{{ID: "application-1", ProgramIDs: []string{"targetport"}, StartedAt: started, CompletedAt: started.Add(time.Second), Result: PlanApplicationResult{Applied: true, ActionsApplied: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}}}
	receipts := ProgramRepairReceipts(applications, nil)
	if len(receipts) != 1 || !receipts[0].Success || len(receipts[0].Resources) != 1 || receipts[0].Resources[0] != ref || receipts[0].StartedAt != started {
		t.Fatalf("controller receipt: %+v", receipts)
	}
}

func TestRefusedActionReceiptDoesNotAttributeDeniedPeerResources(t *testing.T) {
	first := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "first"}
	second := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "denied"}
	actions := []program.Action{program.DeleteObject{Ref: first, Reason: "first"}, program.DeleteObject{Ref: second, Reason: "second"}}
	receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "partial", Error: "forbidden", Result: PlanApplicationResult{Applied: true, Plan: program.Plan{Actions: actions}, ActionsApplied: actions[:1], ActionReports: []PlanActionReport{{ActionIndex: 0, Status: "applied"}, {ActionIndex: 1, Status: "refused", FailureReason: "authorization_denied"}}}}}, nil)
	denied := false
	for _, receipt := range receipts {
		if receipt.FailureReason == "authorization_denied" {
			if len(receipt.Resources) != 1 || receipt.Resources[0] != second || receipt.Success {
				t.Fatalf("denied evidence attributed to another action: %+v", receipt)
			}
			denied = true
		}
	}
	if !denied {
		t.Fatal("missing denied action receipt")
	}
}
