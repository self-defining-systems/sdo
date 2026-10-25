package runtime

import (
	"context"
	"testing"
	"time"

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

func TestUnexplainedRemovalChecksExecutionState(t *testing.T) {
	for _, change := range []string{"none", "uid", "version", "accepted-source", "acknowledged", "owned", "control", "incident", "accepted-benign"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
			object := urObject(ref)
			store := &inventoryStore{objects: []*unstructured.Unstructured{object}}
			index := NewSourceInventory("app", "accepted", nil)
			c := &Controller{config: ControllerConfig{Namespace: "app", SourceCommit: "accepted"}, currentIncidentRequest: &IncidentRequest{IncidentID: "incident"}, sourceInventory: index, drift: &DriftControllerConfig{Inventory: true, Events: store}}
			options, err := c.unexplainedRemovals("incident", "control")
			if err != nil || len(options) != 1 {
				t.Fatalf("missing option: %+v %v", options, err)
			}
			client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object.DeepCopy()}}}
			switch change {
			case "uid":
				store.objects[0].SetUID("recreated")
			case "version":
				store.objects[0].SetResourceVersion("8")
			case "accepted-source":
				c.sourceInventory = NewSourceInventory("app", "accepted", []*unstructured.Unstructured{object})
			case "acknowledged":
				c.sourceInventory.acknowledged = map[sdk.ObjectRef]bool{ref: true}
			case "owned":
				store.objects[0].SetLabels(map[string]string{"app.kubernetes.io/managed-by": "sdo"})
			case "control":
				ref.Namespace = "control"
				store.objects[0].SetNamespace("control")
			case "incident":
				c.currentIncidentRequest.IncidentID = "other"
			case "accepted-benign":
				c.acceptedSourceDrift = acceptInventoryFindings(index.Findings(store.objects, time.Now()))
			}
			selected := options[0]
			err = c.removeUnexplained(ctx, "incident", "control", selected, client, selected.Ref, selected.UID, selected.ResourceVersion)
			if change == "none" {
				if err != nil || client.deletes != 1 {
					t.Fatalf("valid delete: %v", err)
				}
			} else if err == nil || client.deletes != 0 {
				t.Fatalf("unsafe deletion (%s): %v", change, err)
			}
		})
	}
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
