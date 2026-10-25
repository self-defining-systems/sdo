package program_test

import (
	"encoding/json"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestGeneralPatchVocabulary(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "svc"}
	object := map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "svc", "namespace": "app"}, "spec": map[string]any{"ports": []any{map[string]any{"name": "keep", "port": float64(80)}, map[string]any{"name": "remove", "port": float64(90)}}}}
	for _, mode := range []string{"remove", "replace", "insert"} {
		t.Run(mode, func(t *testing.T) {
			a := program.PatchObject{Ref: ref, Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "remove"}, Op: mode, Value: map[string]any{"name": "new", "port": float64(91)}}}}
			next, err := program.ProjectPatch(object, a)
			if err != nil {
				t.Fatal(err)
			}
			ports := next["spec"].(map[string]any)["ports"].([]any)
			if ports[0].(map[string]any)["name"] != "keep" {
				t.Fatal("unrelated port changed")
			}
			want := 1
			if mode == "replace" {
				want = 2
			}
			if mode == "insert" {
				want = 3
			}
			if len(ports) != want {
				t.Fatalf("got %v", ports)
			}
		})
	}
	original, _ := json.Marshal(object)
	_, err := program.ProjectPatch(object, program.PatchObject{Ref: ref, Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "absent"}, Op: "remove"}}})
	if err == nil {
		t.Fatal("missing identity accepted")
	}
	after, _ := json.Marshal(object)
	if !reflect.DeepEqual(original, after) {
		t.Fatal("failed patch mutated input")
	}
	for _, a := range []program.PatchObject{
		{Ref: ref, Type: "json", Patch: json.RawMessage(`[{"op":"remove","path":"/spec/ports/1"}]`)},
		{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"fixed":"yes"}}}`)},
		{Ref: ref, Type: "strategic", Patch: json.RawMessage(`{"spec":{"ports":[{"port":90,"$patch":"delete"}]}}`)},
	} {
		if _, err := program.ProjectPatch(object, a); err != nil {
			t.Fatal(err)
		}
	}
}
func TestGeneralSecretBoundary(t *testing.T) {
	for _, path := range []string{"/api/v1/namespaces/app/secrets/x", "/api/v1/secrets", "/api/v1/namespaces/app/%73ecrets/x", "https://evil.test/api/v1/pods", "/api/v1/../v1/secrets"} {
		if (program.KubernetesRequest{Method: "GET", Path: path}).Validate() == nil {
			t.Fatalf("accepted forbidden path %s", path)
		}
	}
}
