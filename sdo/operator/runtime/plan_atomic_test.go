package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type resourceAdmissionClient struct {
	*planFakeClient
	attempts int
}

func (c *resourceAdmissionClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.attempts++
	before, _ := json.Marshal(c.objects[ref].Object)
	after, err := jsonpatch.MergePatch(before, patch)
	if err != nil {
		return err
	}
	var workload struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Resources struct{ Requests, Limits map[string]string }
					}
				}
			}
		}
	}
	if err = json.Unmarshal(after, &workload); err != nil {
		return err
	}
	for _, container := range workload.Spec.Template.Spec.Containers {
		req, lim := container.Resources.Requests["memory"], container.Resources.Limits["memory"]
		if req != "" && lim != "" {
			r, l := resource.MustParse(req), resource.MustParse(lim)
			if r.Cmp(l) > 0 {
				return fmt.Errorf("memory request %s exceeds limit %s", req, lim)
			}
		}
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}

func TestPlanExecutorAtomicCoupledWrites(t *testing.T) {
	for _, mode := range []string{"fields", "patch-and-field", "nonadjacent"} {
		t.Run(mode, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			obj := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "api", "command": []any{"serve"}, "resources": map[string]any{"requests": map[string]any{"memory": "8Mi"}, "limits": map[string]any{"memory": "10Mi"}}}, map[string]any{"name": "sidecar", "image": "v1"}}}}})
			obj.SetUID("original")
			obj.SetResourceVersion("1")
			client := &resourceAdmissionClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: obj}}}
			prefix := "spec.template.spec.containers[name=api].resources."
			actions := []program.Action{program.SetField{Ref: ref, Path: prefix + "requests.memory", Value: testPlanValue("128Mi")}, program.SetField{Ref: ref, Path: prefix + "limits.memory", Value: testPlanValue("256Mi")}}
			if mode == "patch-and-field" {
				actions[0] = program.PatchObject{Ref: ref, Type: "strategic", Patch: json.RawMessage(`{"spec":{"template":{"spec":{"containers":[{"name":"api","resources":{"requests":{"memory":"128Mi"}}}]}}}}`)}
			}
			if mode == "nonadjacent" {
				other := ref
				other.Name = "other"
				client.objects[other] = obj.DeepCopy()
				client.objects[other].SetName(other.Name)
				actions = []program.Action{actions[0], program.SetField{Ref: other, Path: prefix + "limits.memory", Value: testPlanValue("256Mi")}, actions[1]}
			}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
			if err != nil || !result.Applied || len(result.ActionsApplied) != len(actions) {
				t.Fatalf("atomic plan failed: %+v %v", result, err)
			}
			want := 1
			if mode == "nonadjacent" {
				want = 2
			}
			if client.attempts != want {
				t.Fatalf("got %d writes, want %d", client.attempts, want)
			}
			command, _ := program.FieldValue(client.objects[ref].Object, "spec.template.spec.containers[name=api].command[0]")
			if command != "serve" {
				t.Fatal("command dropped")
			}
			if err = executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			got, _ := program.FieldValue(client.objects[ref].Object, prefix+"limits.memory")
			if got != "10Mi" {
				t.Fatalf("rollback lost preimage: %v", got)
			}
		})
	}
}
