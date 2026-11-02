package replay

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type inventoryRow struct{ Operation, Primitive, Replay, Exclusion string }
type inventoryVerifier struct{}

func (inventoryVerifier) Verify(context.Context) (bool, error) { return true, nil }

type inventoryRollouts struct{}

func (inventoryRollouts) Wait(context.Context, []sdk.ObjectRef) error { return nil }

// The list is intentionally explicit: a new inventory operation cannot pass by
// inheriting a generic nonempty "covered" flag. Every row executes its model.
func TestGeneralKubectlCoverageFloor(t *testing.T) {
	raw, err := os.ReadFile("../../../../docs/sdk-action-inventory.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []inventoryRow
	if json.Unmarshal(raw, &rows) != nil {
		t.Fatal("invalid inventory")
	}
	covered := map[string]func(*testing.T){}
	for _, op := range []string{"apply", "create", "replace", "patch-json", "patch-merge", "patch-strategic", "patch-list-match", "delete", "scale", "rollout-restart", "rollout-undo", "rollout-pause", "rollout-resume", "set-image", "set-env", "set-resources", "label", "annotate", "taint", "cordon", "autoscale", "wait"} {
		covered[op] = func(t *testing.T) { checkInventoryObject(t, op) }
	}
	for _, op := range []string{"exec", "cp", "container-admin", "container-reload", "container-files"} {
		covered[op] = func(t *testing.T) { testGeneralExternalReplay(t, op) }
	}
	covered["in-cluster-http"] = checkInventoryHTTP
	covered["one-off-job"] = checkInventoryJob
	covered["drain"] = checkInventoryEviction
	for _, op := range []string{"get", "describe", "events", "logs", "top"} {
		covered[op] = func(t *testing.T) { checkInventoryRead(t, op) }
	}
	expected := map[string]string{}
	for op := range covered {
		expected[op] = "PatchObject"
	}
	for _, op := range []string{"apply", "create", "replace", "autoscale"} {
		expected[op] = "ApplyObject"
	}
	for _, op := range []string{"exec", "cp", "container-admin", "container-reload", "container-files"} {
		expected[op] = "EnsureExec"
	}
	for op, primitive := range map[string]string{"delete": "DeleteObject", "wait": "WaitFor", "in-cluster-http": "EnsureHTTP", "one-off-job": "RunJob", "drain": "KubernetesRequest", "top": "KubernetesRead", "get": "Object", "describe": "Object+Events", "logs": "Logs", "events": "Events"} {
		expected[op] = primitive
	}
	seen := map[string]bool{}
	for _, row := range rows {
		t.Run(row.Operation, func(t *testing.T) {
			if row.Operation == "" || seen[row.Operation] {
				t.Fatal("missing or duplicate operation")
			}
			seen[row.Operation] = true
			if row.Exclusion != "" {
				if row.Operation != "secret-data-read" || row.Primitive != "" || !strings.Contains(row.Exclusion, "Secret") {
					t.Fatal("unreviewed coverage exclusion")
				}
				return
			}
			if row.Primitive == "" || row.Replay == "" {
				t.Fatal("operation lacks primitive or replay contract")
			}
			if row.Primitive != expected[row.Operation] {
				t.Fatalf("inventory primitive %s does not match tested %s", row.Primitive, expected[row.Operation])
			}
			run, ok := covered[row.Operation]
			if !ok {
				t.Fatal("operation has no executable replay case")
			}
			run(t)
		})
	}
	for op := range covered {
		if !seen[op] {
			t.Fatalf("coverage operation %s disappeared from inventory", op)
		}
	}
}
func inventoryObject(ref sdk.ObjectRef, spec map[string]any) unstructured.Unstructured {
	metadata := map[string]any{"name": ref.Name}
	if ref.Namespace != "" {
		metadata["namespace"] = ref.Namespace
	}
	object := unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": metadata}}
	if spec != nil {
		object.Object["spec"] = spec
	}
	return object
}
func checkInventoryObject(t *testing.T, op string) {
	t.Helper()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	object := inventoryObject(ref, nil)
	object.Object["data"] = map[string]any{"keep": "yes", "state": "before"}
	path := "/data/state"
	var want any = "after"
	var action program.Action
	var patch string
	mode := "merge"
	missing, deleted := false, false
	switch op {
	case "apply", "create", "replace":
		desired := inventoryObject(ref, nil)
		desired.Object["data"] = map[string]any{"state": "after"}
		missing = op == "create"
		action = program.ApplyObject{Ref: ref, Object: desired.Object, Mode: op}
	case "delete":
		action = program.DeleteObject{Ref: ref, Reason: "inventory fixture"}
		deleted = true
	case "patch-json":
		mode = "json"
		patch = `[{"op":"replace","path":"/data/state","value":"after"},{"op":"remove","path":"/data/keep"}]`
	case "patch-merge":
		patch = `{"data":{"state":"after"}}`
	case "label":
		patch = `{"metadata":{"labels":{"inventory":"after"}}}`
		path = "/metadata/labels/inventory"
	case "annotate":
		patch = `{"metadata":{"annotations":{"inventory":"after"}}}`
		path = "/metadata/annotations/inventory"
	case "wait":
		action = program.WaitFor{Ref: ref, Path: path, Equals: "before", Timeout: time.Millisecond}
		want = "before"
	case "patch-list-match":
		ref.Kind = "Service"
		object = inventoryObject(ref, map[string]any{"ports": []any{map[string]any{"name": "keep", "port": int64(80)}, map[string]any{"name": "drop", "port": int64(90)}}})
		action = program.PatchObject{Ref: ref, Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "drop"}, Op: "remove"}}}
		path = "/spec/ports"
		want = []any{map[string]any{"name": "keep", "port": int64(80)}}
	case "taint", "cordon":
		ref = sdk.ObjectRef{APIVersion: "v1", Kind: "Node", Name: "worker"}
		object = inventoryObject(ref, map[string]any{})
		if op == "cordon" {
			patch = `{"spec":{"unschedulable":true}}`
			path = "/spec/unschedulable"
			want = true
		} else {
			patch = `{"spec":{"taints":[{"key":"maintenance","effect":"NoSchedule"}]}}`
			path = "/spec/taints/0/key"
			want = "maintenance"
		}
	case "autoscale":
		ref = sdk.ObjectRef{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler", Namespace: "app", Name: "api"}
		desired := inventoryObject(ref, map[string]any{"minReplicas": int64(1), "maxReplicas": int64(4), "scaleTargetRef": map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "api"}})
		action = program.ApplyObject{Ref: ref, Object: desired.Object, Mode: "create"}
		missing = true
		path = "/spec/maxReplicas"
		want = int64(4)
	default:
		ref = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
		object = inventoryObject(ref, map[string]any{"replicas": int64(1), "template": map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "api"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "broken", "env": []any{map[string]any{"name": "KEEP", "value": "yes"}}}}}}})
		switch op {
		case "scale":
			patch = `{"spec":{"replicas":3}}`
			path = "/spec/replicas"
			want = int64(3)
		case "rollout-restart":
			patch = `{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"after"}}}}}`
			path = "/spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt"
		case "rollout-pause", "rollout-resume":
			want = op == "rollout-pause"
			body, _ := json.Marshal(map[string]any{"spec": map[string]any{"paused": want}})
			patch = string(body)
			path = "/spec/paused"
		case "set-image", "rollout-undo", "patch-strategic":
			mode = "strategic"
			patch = `{"spec":{"template":{"spec":{"containers":[{"name":"app","image":"after"}]}}}}`
			path = "/spec/template/spec/containers/0/image"
		case "set-env":
			mode = "strategic"
			patch = `{"spec":{"template":{"spec":{"containers":[{"name":"app","env":[{"name":"ADD","value":"after"}]}]}}}}`
			path = "/spec/template/spec/containers/0/env/0/value"
		case "set-resources":
			mode = "strategic"
			patch = `{"spec":{"template":{"spec":{"containers":[{"name":"app","resources":{"limits":{"memory":"64Mi"}}}]}}}}`
			path = "/spec/template/spec/containers/0/resources/limits/memory"
			want = "64Mi"
		default:
			t.Fatalf("unimplemented operation %s", op)
		}
	}
	if action == nil {
		action = program.PatchObject{Ref: ref, Type: mode, Patch: json.RawMessage(patch)}
	}
	s := sdktest.Snapshot{NamespaceName: "app"}
	if !missing {
		s.Objects = []unstructured.Unstructured{object}
	}
	c, err := newCluster(s)
	if err != nil {
		t.Fatal(err)
	}
	e := operator.PlanExecutor{Client: c, Rollouts: inventoryRollouts{}, Verifier: inventoryVerifier{}}
	r, err := e.Apply(ctx, operator.IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
	if err != nil || !r.VerificationPassed {
		t.Fatalf("%s production/replay failed: %+v %v", op, r, err)
	}
	got, err := c.Get(ctx, ref)
	if deleted {
		if !apierrors.IsNotFound(err) {
			t.Fatalf("delete not replayed: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	actual, err := program.FieldValue(got.Object, path)
	if err != nil || !sameRecordedJSON(mustInventoryJSON(t, actual), mustInventoryJSON(t, want)) {
		t.Fatalf("%s result %s = %#v want %#v: %v", op, path, actual, want, err)
	}
	if err = checkWrites(ctx, c, r); err != nil {
		t.Fatal(err)
	}
}
func mustInventoryJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func checkInventoryHTTP(t *testing.T)            { t.Helper(); TestGeneralHTTPReplayRecordedTransition(t) }
func checkInventoryJob(t *testing.T)             { t.Helper(); TestGeneralJobReplayRecordedCompletion(t) }
func checkInventoryEviction(t *testing.T)        { t.Helper(); TestGeneralAPIReplayRecordedEffect(t) }
func checkInventoryRead(t *testing.T, op string) { t.Helper(); checkGeneralReadReplay(t, op) }
