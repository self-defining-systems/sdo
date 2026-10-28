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

func TestPlanExecutorAtomicLostReceiptReconcilesWholeObject(t *testing.T) {
	ref, plan, client := repeatedReplicaPlan()
	client.loseReceipt = true
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, plan, false)
	if err == nil || result.PendingAction == nil {
		t.Fatalf("lost receipt did not retain pending aggregate: %v", err)
	}
	result, err = executor.Reconcile(context.Background(), result)
	if err != nil || len(result.ActionsApplied) != len(plan.Actions) || client.attempts != 1 {
		t.Fatalf("whole-object receipt lost: actions=%d attempts=%d err=%v", len(result.ActionsApplied), client.attempts, err)
	}
	if err = executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	requireRecoveredReplicas(t, client, ref)
	if client.attempts != 2 {
		t.Fatalf("rollback used %d writes, want one forward + one reverse", client.attempts)
	}
}

func TestPlanExecutorAtomicSingletonGeneralRollback(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			a := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "a"}
			b := a
			b.Name = "b"
			c := a
			c.Name = "c"
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
			for _, ref := range []sdk.ObjectRef{a, b, c} {
				o := planObject(ref, nil)
				o.Object["data"] = map[string]any{"x": "old"}
				o.SetLabels(map[string]string{"state": "old"})
				o.SetUID(types.UID("uid-" + ref.Name))
				o.SetResourceVersion("1")
				client.objects[ref] = o
			}
			actions := []program.Action{program.PatchObject{Ref: a, Type: "merge", Patch: json.RawMessage(`{"data":{"x":"new"}}`)}, program.PatchObject{Ref: a, Type: "merge", Patch: json.RawMessage(`{"data":{"y":"added"}}`)}, program.PatchObject{Ref: b, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"state":"new"}}}`)}}
			if partial {
				actions = append(actions, program.PatchObject{Ref: c, Type: "merge", Patch: json.RawMessage(`{"data":{"x":"new"}}`)})
			}
			executor := PlanExecutor{Client: &singletonFailureClient{planFakeClient: client, fail: partial, target: c}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
			if partial {
				if err == nil {
					t.Fatal("expected injected failure")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err = executor.Rollback(context.Background(), result); err != nil {
					t.Fatal(err)
				}
			}
			if got := client.objects[b].GetLabels()["state"]; got != "old" {
				t.Fatalf("singleton metadata not restored: %s (partial=%v)", got, partial)
			}
		})
	}
}

type singletonFailureClient struct {
	*planFakeClient
	fail   bool
	target sdk.ObjectRef
}

func (c *singletonFailureClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.fail && ref == c.target {
		return fmt.Errorf("injected write failure")
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}

func TestPlanExecutorApplyNamedServicePortNumberChange(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object := planObject(ref, map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(80), "targetPort": int64(8000)}, map[string]any{"name": "other", "port": int64(90)}}})
	object.SetUID("original")
	object.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	desired := planObject(ref, map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(8080)}}})
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ApplyObject{Ref: ref, Object: desired.Object}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	ports, _ := program.FieldValue(client.objects[ref].Object, "spec.ports")
	if len(ports.([]any)) != 2 {
		t.Fatalf("duplicate named ports: %v", ports)
	}
	for path, want := range map[string]any{"spec.ports[name=http].port": int64(8080), "spec.ports[name=http].targetPort": int64(8000), "spec.ports[name=other].port": int64(90)} {
		got, e := program.FieldValue(client.objects[ref].Object, path)
		if e != nil || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: %v %v", path, got, e)
		}
	}
	if err = executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	port, _ := program.FieldValue(client.objects[ref].Object, "spec.ports[name=http].port")
	if fmt.Sprint(port) != "80" {
		t.Fatalf("port not restored: %v", port)
	}
}
