package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestAtomicServicePortRollbackPreservesConcurrentAddition(t *testing.T) {
	for _, single := range []bool{false, true} {
		name := "remaining-port"
		if single {
			name = "empty-list-omitted"
		}
		t.Run(name, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			ports := []any{map[string]any{"name": "rpc", "port": int64(8300)}}
			expected := map[string]int64{"rpc": 8300, "concurrent": 9000}
			if !single {
				ports = append(ports, map[string]any{"name": "http", "port": int64(8500)})
				expected["http"] = 8500
			}
			object := planObject(ref, map[string]any{"selector": map[string]any{"app": "old"}, "ports": ports})
			object.SetUID("original")
			object.SetResourceVersion("1")
			client := &concurrentServicePortClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
				program.SetField{Ref: ref, Path: "spec.selector.app", Value: testPlanValue("new")},
				program.RemoveServicePort{Ref: ref, Name: "rpc", AllowEmpty: single},
			}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("apply: %+v %v", result, err)
			}
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			restored, _, _ := unstructured.NestedSlice(client.objects[ref].Object, "spec", "ports")
			if len(restored) != len(expected) {
				t.Fatalf("rollback lost a port: %+v", restored)
			}
			for _, value := range restored {
				port := value.(map[string]any)
				name := port["name"].(string)
				if want, exists := expected[name]; !exists || port["port"] != want {
					t.Fatalf("unexpected restored port: %+v", port)
				}
				delete(expected, name)
			}
			if len(expected) != 0 {
				t.Fatalf("ports not restored: %+v", expected)
			}
			selector, _, _ := unstructured.NestedString(client.objects[ref].Object, "spec", "selector", "app")
			if selector != "old" {
				t.Fatalf("selector not restored: %s", selector)
			}
		})
	}
}
