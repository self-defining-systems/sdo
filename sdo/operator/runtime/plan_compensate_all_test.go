package runtime

import (
	"context"
	"errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

type interruptedCompensationClient struct {
	*lifecycleTestClient
	failRestore bool
}

func (c *interruptedCompensationClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.failRestore && ref.Name == "second" && strings.Contains(string(patch), `"before"`) {
		return errors.New("synthetic compensation interruption")
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
func TestPlanCompensationAttemptsEveryReversibleWrite(t *testing.T) {
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	var actions []program.Action
	var safety *PlanSafetyPolicy
	var first sdk.ObjectRef
	for _, name := range []string{"first", "second"} {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: name}
		if name == "first" {
			first = ref
		}
		before := planObject(ref, nil)
		delete(before.Object, "spec")
		before.SetUID("fixture-uid")
		before.SetResourceVersion("1")
		before.Object["data"] = map[string]any{"key": "before"}
		after := before.DeepCopy()
		after.Object["data"].(map[string]any)["key"] = "after"
		proof := fixturePlanSafety(t, ref, before, after)
		if safety == nil {
			safety = proof
		} else {
			safety.Objects[ref] = proof.Objects[ref]
		}
		objects[ref] = before
		actions = append(actions, program.SetField{Ref: ref, Path: "data[key]", Value: "after"})
	}
	np := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	object := planObject(np, map[string]any{"podSelector": map[string]any{}})
	object.SetUID("fixture-uid")
	object.SetResourceVersion("1")
	objects[np] = object
	safety.Objects[np] = PlanObjectSafety{UID: "fixture-uid", DeleteAllowed: true, Digests: map[string]bool{planSafetyDigest(object): true}}
	actions = append(actions, program.DeleteObject{Ref: np, Reason: "synthetic fixture"})
	client := &interruptedCompensationClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: objects}, ignoreDelete: true}, failRestore: true}
	e := PlanExecutor{Client: client, Source: lifecycleTestSource{}, Safety: safety, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
	if err == nil || result.RolledBack || !strings.Contains(result.RollbackError, "irreversible") {
		t.Fatal("partial compensation not disclosed")
	}
	if client.objects[first].Object["data"].(map[string]any)["key"] != "before" {
		t.Fatal("one failed compensation stranded an earlier reversible write")
	}
	client.failRestore = false
	if err := e.rollback(context.Background(), &result); err == nil || !strings.Contains(err.Error(), "not restored") {
		t.Fatal("retry hid irreversible deletion")
	}
	if result.RollbackCompleted != len(actions) || result.RolledBack || client.creates != 0 {
		t.Fatal("compensation retry lost progress or recreated deleted object")
	}
}
