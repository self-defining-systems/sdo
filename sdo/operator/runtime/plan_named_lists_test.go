package runtime

import (
	"context"
	"encoding/json"
	"strconv"
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
				want, exists := expected[name]
				got, err := json.Marshal(port["port"])
				if !exists || err != nil || string(got) != strconv.FormatInt(want, 10) {
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

func TestRemoveServicePortRejectsReintroducedMember(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	removed := map[string]any{"name": "rpc", "port": int64(8300)}
	object := planObject(ref, map[string]any{"ports": []any{removed, map[string]any{"name": "http", "port": int64(8500)}}})
	client, ref := newServerPlanClient(object)
	client.afterPatch = func(live *unstructured.Unstructured) {
		ports, _, _ := unstructured.NestedSlice(live.Object, "spec", "ports")
		ports = append([]any{removed}, ports...)
		_ = unstructured.SetNestedSlice(live.Object, ports, "spec", "ports")
	}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"},
		program.Plan{Actions: []program.Action{program.RemoveServicePort{Ref: ref, Name: "rpc"}}}, false)
	if err == nil || result.VerificationPassed {
		t.Fatalf("reintroduced removed member was credited: %+v %v", result, err)
	}
}
