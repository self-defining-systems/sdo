package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestAtomicServicePortRollbackPreservesConcurrentAddition(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object := planObject(ref, map[string]any{
		"selector": map[string]any{"app": "old"},
		"ports":    []any{map[string]any{"name": "rpc", "port": int64(8300)}, map[string]any{"name": "http", "port": int64(8500)}},
	})
	object.SetUID("original")
	object.SetResourceVersion("1")
	client := &concurrentServicePortClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: ref, Path: "spec.selector.app", Value: testPlanValue("new")},
		program.RemoveServicePort{Ref: ref, Name: "rpc"},
	}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("apply: %+v %v", result, err)
	}
	if err := executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	ports, _, _ := unstructured.NestedSlice(client.objects[ref].Object, "spec", "ports")
	if len(ports) != 3 {
		t.Fatalf("rollback lost a port: %+v", ports)
	}
	selector, _, _ := unstructured.NestedString(client.objects[ref].Object, "spec", "selector", "app")
	if selector != "old" {
		t.Fatalf("selector not restored: %s", selector)
	}
	for _, port := range ports {
		item := port.(map[string]any)
		if item["name"] == "concurrent" && item["port"] == int64(9000) {
			return
		}
	}
	t.Fatalf("concurrent port changed: %+v", ports)
}
