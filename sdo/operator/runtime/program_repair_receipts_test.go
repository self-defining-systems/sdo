package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestProgramApplicationsProduceControllerRepairReceipts(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	started := time.Unix(1700000000, 0).UTC()
	applications := []RecordedPlanApplication{{ID: "application-1", ProgramIDs: []string{"targetport"}, StartedAt: started, CompletedAt: started.Add(time.Second), Result: PlanApplicationResult{Applied: true, VerificationCompleted: true, VerificationPassed: true, ActionsApplied: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}}}
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

// Completion includes waits and satisfied ensures, but their observation refs
// must never become causal write scope. Exercise both action orderings through
// the real executor, then consume its immutable mutation evidence.
func TestProgramRepairReceiptsExcludeSuccessfulObservationScopes(t *testing.T) {
	for _, first := range []bool{false, true} {
		t.Run(map[bool]string{false: "observe-after-write", true: "observe-before-write"}[first], func(t *testing.T) {
			written := databaseAction().Target.Pod
			watched := written
			watched.Name = "synthetic-observed-peer"
			object := planObject(written, map[string]any{})
			object.SetUID("written")
			object.SetResourceVersion("1")
			peer := planObject(watched, map[string]any{})
			peer.SetUID("observed")
			peer.SetResourceVersion("1")
			peer.SetLabels(map[string]string{"ready": "yes"})
			client := &externalTestClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{written: object, watched: peer}}}}
			write := program.PatchObject{Ref: written, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"repaired":"yes"}}}`)}
			observations := []program.Action{program.EnsureExec{Observe: program.Command{Pod: watched, Container: "db", Args: []string{"observe"}}, Ensure: program.Command{Pod: watched, Container: "db", Args: []string{"ensure"}}}, program.WaitFor{Ref: watched, Path: "/metadata/labels/ready", Equals: "yes"}}
			actions := append([]program.Action{write}, observations...)
			if first {
				actions = append(observations, write)
			}
			executor := PlanExecutor{Client: client, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
			result, err := executor.Apply(context.Background(), IncidentRequest{}, program.Plan{Actions: actions}, false)
			if err != nil {
				t.Fatal(err)
			}
			if !PlanActionsComplete(result) || len(result.ActionsApplied) != 1 {
				t.Fatalf("producer did not complete one write and two observations: %+v", result)
			}
			receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "synthetic", Result: result}}, nil)
			if len(receipts) != 1 || !receipts[0].Success || len(receipts[0].Resources) != 1 || receipts[0].Resources[0] != written {
				t.Fatalf("observation acquired write attribution: %+v", receipts)
			}
		})
	}
}
