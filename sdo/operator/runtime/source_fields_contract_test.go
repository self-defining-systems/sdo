package runtime

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestReconcileRestoresAllSourceConfigurationFields(t *testing.T) {
	for _, field := range []string{"rules", "roleRef", "subjects", "aggregationRule", "webhooks", "subsets", "type", "immutable", "futureCustomField"} {
		t.Run(field, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "app-cleanup"}
			source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name}, field: []any{map[string]any{"configured": "accepted"}}}}
			live := source.DeepCopy()
			live.Object[field] = []any{map[string]any{"configured": "broken"}}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("reconcile: %+v %v", result, err)
			}
			if !reflect.DeepEqual(client.objects[ref].Object[field], source.Object[field]) {
				t.Fatalf("source field %s was not restored: %v", field, client.objects[ref].Object[field])
			}
			if err := executor.rollback(context.Background(), &result); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			if !reflect.DeepEqual(client.objects[ref].Object[field], live.Object[field]) {
				t.Fatalf("rollback lost original %s", field)
			}
		})
	}
}

func TestSourceReconcilePreservesExternalAnnotations(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
	source := planObject(ref, map[string]any{"replicas": int64(1)})
	live := source.DeepCopy()
	live.SetAnnotations(map[string]string{"another-controller.example/completed": "true"})
	next, err := sourceFields(live, source, ref)
	if err != nil || next.GetAnnotations()["another-controller.example/completed"] != "true" {
		t.Fatalf("external annotation erased: %v %v", next, err)
	}
}

func TestSourceDiffPreservesIndependentCompletionAndDetectsMissingController(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
	source := planObject(ref, map[string]any{"replicas": int64(1)})
	live := source.DeepCopy()
	live.SetAnnotations(map[string]string{"another-controller.example/completed": "true"})
	snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}}
	view := LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed {
		t.Fatalf("external completion annotation flagged: %+v %v", diff, err)
	}
	snapshot.Objects = nil
	view.DetectionContext = snapshot
	if diff, err := view.DiffAgainstSource(ref); err != nil || !diff.Changed || diff.SourceAbsent {
		t.Fatalf("deleted source controller invisible: %+v %v", diff, err)
	}
}

func TestSourceReconcilePreservesUndeclaredNestedServerFields(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	source := planObject(ref, map[string]any{"selector": map[string]any{"app": "api"}})
	live := planObject(ref, map[string]any{"clusterIP": "10.0.0.1", "selector": map[string]any{"app": "wrong"}})
	next, err := sourceFields(live, source, ref)
	if err != nil {
		t.Fatal(err)
	}
	ip, _, _ := unstructured.NestedString(next.Object, "spec", "clusterIP")
	selector, _, _ := unstructured.NestedString(next.Object, "spec", "selector", "app")
	if ip != "10.0.0.1" || selector != "api" {
		t.Fatalf("source reconcile lost server fields or source values: %v", next)
	}
}
