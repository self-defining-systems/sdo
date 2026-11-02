package runtime

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type deniedDeleteClient struct{ *lifecycleTestClient }

func (c deniedDeleteClient) Delete(_ context.Context, ref sdk.ObjectRef, _ types.UID, _ string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: ref.Kind}, ref.Name, nil)
}

func TestDeletionReceiptRequiresConfirmedAbsence(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: urObject(ref)}}, ignoreDelete: true}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "remove injected object"}}}, false)
	if err == nil {
		t.Fatal("unconfirmed deletion succeeded")
	}
	receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "unknown-delete", Result: result, Error: err.Error()}}, nil)
	if len(receipts) != 1 || receipts[0].Success {
		t.Fatalf("unconfirmed delete claimed success: %+v", receipts)
	}
}

func TestDeniedDeleteMenuReturnsTypedFailure(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
	client := deniedDeleteClient{&lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: urObject(ref)}}}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "denied", Namespace: "app"}, nil, nil, nil, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	if menu.Error != "" {
		t.Fatal(menu.Error)
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "DeleteObject", RoundLimit: 1}, Parameters: map[string]string{"ref": "ServiceAccount/app/cleanup", "reason": "remove injected object"}})
	if response.Report.GetReasonCode() != "authorization_denied" {
		t.Fatalf("denial lost: %+v", response.Report)
	}
	repairs := loop.continuationRepairs(IncidentResult{})
	if len(repairs) != 1 || repairs[0].Receipt.Success {
		t.Fatalf("failed delete missing from feedback: %+v", repairs)
	}
}

func urObject(ref sdk.ObjectRef) *unstructured.Unstructured {
	o := planObject(ref, nil)
	o.SetUID("injected-uid")
	o.SetResourceVersion("7")
	return o
}

func TestAuthorizationDeniedNoProgressNeedsExactBrokenSet(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
	finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", PrimaryResource: ref, Fingerprint: "exact-finding"}
	verification := ToolResponse{Findings: []sdk.Finding{finding}}
	denied := continuationRepair{Receipt: continuationReceipt{Resources: []sdk.ObjectRef{ref}, FailureReason: "authorization_denied"}}
	previous, err := continuationFeedback(1, verification, denied)
	if err != nil {
		t.Fatal(err)
	}
	if !authorizationDeniedNoProgress(string(previous), verification, []continuationRepair{denied}) {
		t.Fatal("identical denied set did not stop")
	}
	if authorizationDeniedNoProgress("", verification, []continuationRepair{denied}) {
		t.Fatal("first response stopped before continuation")
	}
	changed := verification
	changed.Findings = append([]sdk.Finding(nil), verification.Findings...)
	changed.Findings[0].Fingerprint = "changed"
	if authorizationDeniedNoProgress(string(previous), changed, []continuationRepair{denied}) {
		t.Fatal("changed finding was considered no progress")
	}
	denied.Receipt.FailureReason = "execution_failed"
	if authorizationDeniedNoProgress(string(previous), verification, []continuationRepair{denied}) {
		t.Fatal("ordinary failure stopped continuation")
	}
}
