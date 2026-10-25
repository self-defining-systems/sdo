package runtime

import (
	"encoding/json"
	"fmt"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func TestGeneralActionContractAndPersistenceRoundTrip(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
	command := program.Command{Pod: pod, Container: "app", Args: []string{"sh", "-s"}, Stdin: "fixture script"}
	http := program.HTTPRequest{Pod: pod, Container: "app", Method: "POST", URL: "http://api.app.svc/admin", HeaderEnv: map[string]program.CredentialRef{"X-API-Key": {Env: "ADMIN_KEY"}}}
	large := int64(9007199254740993)
	object := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "flags", "namespace": "app"}, "data": map[string]any{"large": large}}
	jobref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "once"}
	actions := []program.Action{
		program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"data":{"x":"y"}}`)},
		program.PatchObject{Ref: ref, Edits: []program.ListEdit{{Path: "/spec/items", Match: map[string]any{"name": "one"}, Op: "replace", Value: map[string]any{"large": large}}}},
		program.ApplyObject{Ref: ref, Object: object},
		program.KubernetesRequest{Ref: ref, Method: "GET", Path: "/api/v1/namespaces/app/configmaps/flags"},
		program.WaitFor{Ref: ref, Path: "/data/large", Equals: large, Timeout: time.Second, Interval: time.Millisecond},
		program.EnsureExec{Observe: command, Ensure: command},
		program.EnsureHTTP{Observe: http, Ensure: http},
		program.RunJob{Object: program.ApplyObject{Ref: jobref, Object: map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": "once", "namespace": "app"}}}, Completion: program.WaitFor{Ref: jobref, Path: "/status/succeeded", Equals: int64(1)}},
	}
	for _, action := range actions {
		t.Run(actionNameForTest(action), func(t *testing.T) {
			encoded, err := planActionProto(action)
			if err != nil {
				t.Fatal(err)
			}
			decoded := planActionState(encoded)
			if decoded == nil {
				t.Fatal("contract lost action")
			}
			before, _ := json.Marshal(action)
			after, _ := json.Marshal(decoded)
			if !sameGeneralJSON(before, after) {
				t.Fatalf("wire changed action: %s != %s", before, after)
			}
			original := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}}
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var restored PlanApplicationResult
			if err = json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			after, _ = json.Marshal(restored.Plan.Actions[0])
			if !sameGeneralJSON(before, after) {
				t.Fatalf("persistence changed action: %s != %s", before, after)
			}
		})
	}
}
func actionNameForTest(a program.Action) string { return fmt.Sprintf("%T", a) }
func sameGeneralJSON(a, b []byte) bool {
	var x, y any
	if utiljson.Unmarshal(a, &x) != nil || utiljson.Unmarshal(b, &y) != nil {
		return false
	}
	left, _ := json.Marshal(x)
	right, _ := json.Marshal(y)
	return string(left) == string(right)
}
