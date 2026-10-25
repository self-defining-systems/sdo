package program_test

import (
	"encoding/json"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestRemoveServicePortExactIdentityAndPreconditions(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "discovery"}
	for _, tc := range []struct {
		name   string
		ports  string
		action program.RemoveServicePort
		want   string
		fails  bool
	}{
		{"named", `[{"name":"rpc","port":8300},{"name":"http","port":8500,"targetPort":8500}]`, program.RemoveServicePort{Ref: ref, Name: "rpc"}, `[{"name":"http","port":8500,"targetPort":8500}]`, false},
		{"number", `[{"port":53,"protocol":"TCP"},{"port":53,"protocol":"UDP"}]`, program.RemoveServicePort{Ref: ref, Port: 53, Protocol: "TCP"}, `[{"port":53,"protocol":"UDP"}]`, false},
		{"default protocol", `[{"port":53},{"port":54}]`, program.RemoveServicePort{Ref: ref, Port: 53, Protocol: "TCP"}, `[{"port":54}]`, false},
		{"ambiguous", `[{"port":53,"protocol":"TCP"},{"port":53,"protocol":"UDP"}]`, program.RemoveServicePort{Ref: ref, Port: 53}, "", true},
		{"missing", `[{"name":"http","port":8500}]`, program.RemoveServicePort{Ref: ref, Name: "rpc"}, "", true},
		{"duplicate", `[{"name":"rpc","port":8300},{"name":"rpc","port":8301}]`, program.RemoveServicePort{Ref: ref, Name: "rpc"}, "", true},
		{"last", `[{"name":"rpc","port":8300}]`, program.RemoveServicePort{Ref: ref, Name: "rpc"}, "", true},
		{"explicit last", `[{"name":"rpc","port":8300}]`, program.RemoveServicePort{Ref: ref, Name: "rpc", AllowEmpty: true}, `[]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ports []any
			if err := json.Unmarshal([]byte(tc.ports), &ports); err != nil {
				t.Fatal(err)
			}
			object := map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": "app", "name": "discovery"}, "spec": map[string]any{"ports": ports, "selector": map[string]any{"app": "discovery"}}}
			before, _ := json.Marshal(object)
			err := program.ApplyRemoveServicePort(object, tc.action)
			if tc.fails {
				if err == nil {
					t.Fatal("accepted invalid removal")
				}
				after, _ := json.Marshal(object)
				if string(before) != string(after) {
					t.Fatal("failed removal mutated object")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(object["spec"].(map[string]any)["ports"])
			if string(got) != tc.want {
				t.Fatalf("ports %s, want %s", got, tc.want)
			}
			if !reflect.DeepEqual(object["spec"].(map[string]any)["selector"], map[string]any{"app": "discovery"}) {
				t.Fatal("selector changed")
			}
		})
	}
	for _, a := range []program.RemoveServicePort{{Ref: ref}, {Ref: ref, Name: "rpc", Port: 8300}, {Ref: ref, Port: -1}, {Ref: ref, Port: 65536}, {Ref: ref, Name: "rpc", Protocol: "TCP"}, {Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "one"}, Name: "rpc"}} {
		if err := (program.Plan{Actions: []program.Action{a}}).Validate(); err == nil {
			t.Fatalf("accepted invalid action %+v", a)
		}
	}
}
