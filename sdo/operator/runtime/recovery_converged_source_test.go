package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
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

func TestRecoveryExplainsUnconfirmedEffect(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "db"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]any{"name": "db", "namespace": "app", "uid": "u"}, "spec": map[string]any{"replicas": int64(1)}}}
	action := program.ReconcileFromSource{Ref: ref}
	result := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, PreImages: []PlanPreImage{{Ref: ref, Object: object}}, Patches: []PlanPatch{{Ref: ref, Before: object, After: object, Patch: []byte(`{}`)}}}
	loop := &ResponderLoop{}
	absent := func(sdk.ObjectRef) bool { return false }

	changed := object.DeepCopy()
	_ = unstructured.SetNestedField(changed.Object, int64(2), "spec", "replicas")
	got := loop.explainRecoveryPlanFields(result, func(sdk.ObjectRef) *unstructured.Unstructured { return changed }, absent)
	if got.Matched || len(got.Mismatches) != 1 {
		t.Fatalf("explanation = %+v, want one mismatch", got)
	}
	m := got.Mismatches[0]
	if m.GetPath() != "spec.replicas" || m.GetExpected() != "1" || m.GetObserved() != "2" || m.GetRef().GetName() != "db" {
		t.Fatalf("mismatch = %v", m)
	}

	none := loop.explainRecoveryPlanFields(PlanApplicationResult{}, nil, absent)
	if none.Matched || none.Reason == "" {
		t.Fatalf("missing pre-image explanation = %+v", none)
	}

	unreadable := loop.explainRecoveryPlanFields(result, func(sdk.ObjectRef) *unstructured.Unstructured { return nil }, absent)
	if unreadable.Matched || unreadable.Reason == "" {
		t.Fatalf("unreadable object explanation = %+v", unreadable)
	}
}

func TestEffectMismatchRedactsSecretValues(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "s"}
	got := planFieldMismatches(ref, map[string]any{"data.password": "aGVsbG8=", "type": "Opaque"}, map[string]any{"data.password": "d29ybGQ=", "type": "Opaque"})
	if len(got) != 1 || got[0].GetPath() != "data.password" {
		t.Fatalf("mismatches = %v", got)
	}
	for _, value := range []string{got[0].GetExpected(), got[0].GetObserved()} {
		if strings.Contains(value, "aGVsbG8") || strings.Contains(value, "d29ybGQ") || !strings.Contains(value, "redacted") {
			t.Fatalf("secret value leaked: %q", value)
		}
	}
}
