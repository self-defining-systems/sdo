package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Both containers start with the same probe period: positional comparison can
// mistake the untouched container's value for an already compensated repair.
func TestPlanExecutorRollbackRejectsReorderedNamedList(t *testing.T) {
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	container := func(name string) any {
		return map[string]any{"name": name, "image": "toy:v1", "readinessProbe": map[string]any{"periodSeconds": float64(10)}}
	}
	object := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{container("frontend"), container("sidecar")}}}})
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	application, err := executor.Apply(ctx, IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].readinessProbe.periodSeconds", Value: testPlanValue(5)},
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	items, _, err := unstructured.NestedSlice(client.objects[ref].Object, "spec", "template", "spec", "containers")
	if err != nil {
		t.Fatal(err)
	}
	items[0], items[1] = items[1], items[0]
	if err := unstructured.SetNestedSlice(client.objects[ref].Object, items, "spec", "template", "spec", "containers"); err != nil {
		t.Fatal(err)
	}
	writes := len(client.patches)
	if err := executor.Rollback(ctx, application); err == nil {
		value, _ := testField(client.objects[ref], "spec.template.spec.containers.1.readinessProbe.periodSeconds")
		t.Fatalf("rollback reported success after reorder; frontend probe period = %v, want a safe conflict", value)
	}
	if len(client.patches) != writes {
		t.Fatal("conflicting rollback wrote to a reordered list")
	}
}

type refreshFailureProgram struct {
	fastloopPortProgram
	mode     string
	failures atomic.Int32
}

func (p *refreshFailureProgram) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	for _, service := range view.Services() {
		if service.Annotations["synthetic-planning-state"] != "unsupported" {
			continue
		}
		switch p.mode {
		case "panic":
			panic("synthetic refreshed state cannot be planned")
		case "error":
			return program.Plan{}, errors.New("synthetic refreshed state cannot be planned")
		case "empty":
			return program.Plan{}, nil
		case "invalid":
			return program.Plan{Actions: []program.Action{program.SetField{Path: "metadata.name", Value: testPlanValue("wrong")}}}, nil
		case "transient":
			if p.failures.Add(1) == 1 {
				return program.Plan{}, errors.New("synthetic one-shot planning failure")
			}
		}
	}
	return p.fastloopPortProgram.Mitigate(ctx, view, bindings)
}
