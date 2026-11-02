package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// SetField (applied) then DeleteObject whose absence is not confirmed: rollback aborts at the delete,
// stranding the earlier reversible write.
func TestRV13RollbackStrandedByDelete(t *testing.T) {
	cm := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	np := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	cmBefore := planObject(cm, nil)
	delete(cmBefore.Object, "spec")
	cmBefore.SetUID("cm-uid")
	cmBefore.SetResourceVersion("1")
	cmBefore.Object["data"] = map[string]any{"key": "before"}
	cmAfter := cmBefore.DeepCopy()
	cmAfter.Object["data"].(map[string]any)["key"] = "after"
	npBefore := planObject(np, map[string]any{"podSelector": map[string]any{}})
	npBefore.SetUID("np-uid")
	npBefore.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{cm: cmBefore.DeepCopy(), np: npBefore.DeepCopy()}}, ignoreDelete: true}

	e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", Namespace: "app"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: cm, Path: "data[key]", Value: testPlanValue("after"), Reason: "r"},
		program.DeleteObject{Ref: np, Reason: "r"},
	}}, false)
	t.Logf("err=%v rolledBack=%v rollbackErr=%q cm=%v", err, result.RolledBack, result.RollbackError, client.objects[cm].Object["data"])
	if client.objects[cm].Object["data"].(map[string]any)["key"] == "after" {
		t.Fatal("STRANDED: earlier SetField not compensated after later irreversible step failed")
	}
}
