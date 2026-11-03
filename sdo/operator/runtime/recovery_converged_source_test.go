package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestRecoveryConfirmsAlreadyConvergedSource(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "demo"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "demo", "namespace": "app", "uid": "same-object"}, "spec": map[string]any{"replicas": int64(1)}}}
	action := program.ReconcileFromSource{Ref: ref}
	result := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, PreImages: []PlanPreImage{{Ref: ref, Object: object}}, Patches: []PlanPatch{{Ref: ref, Before: object, After: object, Patch: []byte(`{}`)}}}
	loop := &ResponderLoop{}
	get := func(sdk.ObjectRef) *unstructured.Unstructured { return object.DeepCopy() }
	if !loop.recoveryPlanFieldsMatch(result, get, func(sdk.ObjectRef) bool { return false }) {
		t.Fatal("converged source was rejected solely because its desired fields were already applied")
	}
	if finalPlanFieldsMatch(result, get) {
		t.Fatal("unchanged source gained equivalent-repair credit")
	}
	changed := object.DeepCopy()
	_ = unstructured.SetNestedField(changed.Object, int64(2), "spec", "replicas")
	if loop.recoveryPlanFieldsMatch(result, func(sdk.ObjectRef) *unstructured.Unstructured { return changed }, func(sdk.ObjectRef) bool { return false }) {
		t.Fatal("different live fields confirmed source effect")
	}
	changed = object.DeepCopy()
	changed.SetUID("replacement")
	if loop.recoveryPlanFieldsMatch(result, func(sdk.ObjectRef) *unstructured.Unstructured { return changed }, func(sdk.ObjectRef) bool { return false }) {
		t.Fatal("replacement object confirmed old write")
	}
}
