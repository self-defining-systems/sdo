package runtime

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestRemoveServicePortApplyReceiptRollback(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app", "uid": "one", "resourceVersion": "1"}, "spec": map[string]any{"ports": []any{map[string]any{"name": "rpc", "port": int64(8300)}, map[string]any{"name": "http", "port": int64(8500)}}}}}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RemoveServicePort{Ref: ref, Name: "rpc", Reason: "remove unused listener"}}}, false)
	if err != nil || !r.Applied || !r.VerificationPassed {
		t.Fatalf("%+v %v", r, err)
	}
	if len(r.ActionReports) != 1 || r.ActionReports[0].Status != "applied" {
		t.Fatalf("%+v", r.ActionReports)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var restored PlanApplicationResult
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Proto(); err != nil {
		t.Fatal(err)
	}
	if err = e.rollback(context.Background(), &r); err != nil {
		t.Fatal(err)
	}
	ports, _, _ := unstructured.NestedSlice(client.objects[ref].Object, "spec", "ports")
	if len(ports) != 2 {
		t.Fatal("rollback lost port")
	}
}

type concurrentServicePortClient struct{ *planFakeClient }

func (c *concurrentServicePortClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.planFakeClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	if len(c.patches) == 1 {
		ports, _, _ := unstructured.NestedSlice(c.objects[ref].Object, "spec", "ports")
		ports = append(ports, map[string]any{"name": "concurrent", "port": int64(9000)})
		_ = unstructured.SetNestedSlice(c.objects[ref].Object, ports, "spec", "ports")
	}
	return nil
}
func TestRemoveServicePortPreservesConcurrentPortAfterEarlierAction(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app", "uid": "one", "resourceVersion": "1"}, "spec": map[string]any{"selector": map[string]any{"app": "old"}, "ports": []any{map[string]any{"name": "rpc", "port": int64(8300)}, map[string]any{"name": "http", "port": int64(8500)}}}}}
	c := &concurrentServicePortClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	e := PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "selector", ValueKind: program.StringValue, Default: "new"}}}}}
	r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.selector.app", Value: program.FromParam("selector")}, program.RemoveServicePort{Ref: ref, Name: "rpc"}}}, false)
	if err != nil || !r.VerificationPassed {
		t.Fatalf("%+v %v", r, err)
	}
	ports, _, _ := unstructured.NestedSlice(c.objects[ref].Object, "spec", "ports")
	if len(ports) != 2 {
		t.Fatalf("concurrent port lost: %+v", ports)
	}
}
