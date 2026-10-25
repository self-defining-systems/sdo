package runtime

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
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
