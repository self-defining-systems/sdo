package runtime

import (
	"context"
	"reflect"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	view.DetectionContext = clusterEvidenceSnapshot{Snapshot: snapshot, err: apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, ref.Name)}
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

func TestPlanReceiptIgnoresServerMetadataWithoutRepair(t *testing.T) {
	for _, authorization := range []string{"accepted-source", "general"} {
		t.Run(authorization, func(t *testing.T) {
			client, ref := newServerPlanClient(deploymentWithDrift("broken"))
			before := client.objects[ref].DeepCopy()
			after := before.DeepCopy()
			_ = unstructured.SetNestedField(after.Object, int64(2), "spec", "replicas")
			client.bump(client.objects[ref])
			// A readback can capture a newer version even when the repair never landed.
			after.SetResourceVersion(client.objects[ref].GetResourceVersion())
			pending := 0
			result := PlanApplicationResult{
				Plan: reconcilePlan(ref), PendingAction: &pending,
				Patches: []PlanPatch{{Authorization: authorization, Ref: ref, Before: before, After: after}},
			}
			reconciled, err := (PlanExecutor{Client: client}).Reconcile(context.Background(), result)
			if err != nil || reconciled.Applied || len(reconciled.ActionsApplied) != 0 {
				t.Fatalf("server metadata counted as repair: %+v %v", reconciled, err)
			}
		})
	}
}

func TestSourceMetadataRollbackPreservesServerUpdates(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("broken"))
	client.objects[ref].SetLabels(map[string]string{"app": "broken"})
	source := deploymentWithDrift("good")
	source.SetLabels(map[string]string{"app": "accepted"})
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: source}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, reconcilePlan(ref), false)
	if err != nil {
		t.Fatal(err)
	}
	client.bump(client.objects[ref])
	annotations := client.objects[ref].GetAnnotations()
	annotations["external.example/completed"] = "true"
	client.objects[ref].SetAnnotations(annotations)
	if err := executor.rollback(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	if client.objects[ref].GetLabels()["app"] != "broken" || client.objects[ref].GetAnnotations()["external.example/completed"] != "true" {
		t.Fatalf("rollback lost configuration or external metadata: %v", client.objects[ref])
	}
}
