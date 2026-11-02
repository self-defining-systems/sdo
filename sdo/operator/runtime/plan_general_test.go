package runtime

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestGeneralPatchAndApplyProduction(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	client := TypedPlanClient{Client: fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "flags", Namespace: "app", UID: types.UID("uid"), ResourceVersion: "1"}, Data: map[string]string{"keep": "yes", "remove": "yes"}})}
	e := PlanExecutor{Client: client}
	_ = e
	// Directly use the production general dispatcher, with the normal test verifier.
	e.Rollouts = generalNoRollouts{}
	e.Verifier = generalHealthy{}
	a := program.PatchObject{Ref: ref, Type: "json", Patch: json.RawMessage(`[{"op":"remove","path":"/data/remove"},{"op":"add","path":"/metadata/labels","value":{"changed":"true"}}]`)}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("%+v %v", result, err)
	}
	got, _ := client.Get(context.Background(), ref)
	if _, ok, _ := unstructured.NestedFieldNoCopy(got.Object, "data", "remove"); ok {
		t.Fatal("remove ignored")
	}
	a2 := program.ApplyObject{Ref: ref, Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "flags", "namespace": "app"}, "data": map[string]any{"added": "yes"}}}
	if _, err = e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a2}}, false); err != nil {
		t.Fatal(err)
	}
	got, _ = client.Get(context.Background(), ref)
	if v, _, _ := unstructured.NestedString(got.Object, "data", "keep"); v != "yes" {
		t.Fatal("apply lost unrelated key")
	}
}

type generalNoRollouts struct{}

func (generalNoRollouts) Wait(context.Context, []sdk.ObjectRef) error { return nil }

type generalHealthy struct{}

func (generalHealthy) Verify(context.Context) (bool, error) { return true, nil }
func TestGeneralPlanCompensatesMetadataAndPreflightsBatch(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("uid")
	before.SetResourceVersion("1")
	before.Object["data"] = map[string]any{"keep": "yes"}
	c := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}}
	e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
	plan := program.Plan{Actions: []program.Action{
		program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"changed":"yes"}}}`)},
		program.PatchObject{Ref: ref, Type: "json", Patch: json.RawMessage(`[{"op":"remove","path":"/data/absent"}]`)},
	}}
	if _, err := e.Apply(context.Background(), IncidentRequest{}, plan, false); err == nil {
		t.Fatal("invalid batch accepted")
	}
	if len(c.patches) != 0 {
		t.Fatal("preflight wrote before detecting invalid later action")
	}
}
func TestGeneralCreateThenPatchSameObject(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "new"}
	c := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}}
	e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
	p := program.Plan{Actions: []program.Action{
		program.ApplyObject{Ref: ref, Mode: "create", Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "new", "namespace": "app"}, "data": map[string]any{"first": "yes"}}},
		program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"data":{"second":"yes"}}`)},
	}}
	r, err := e.Apply(context.Background(), IncidentRequest{}, p, true)
	if err != nil || len(r.Patches) != 2 || c.creates != 0 {
		t.Fatalf("preflight failed: %+v %v", r, err)
	}
	r, err = e.Apply(context.Background(), IncidentRequest{}, p, false)
	if err != nil || !r.VerificationPassed || len(r.ActionsApplied) != 2 {
		t.Fatalf("apply failed: %+v %v", r, err)
	}
}
