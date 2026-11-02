package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type funcMitigator struct {
	id   string
	plan func(program.ReadView, program.Bindings) (program.Plan, error)
}

func (m funcMitigator) Spec() program.ProgramSpec { return program.ProgramSpec{ID: m.id} }
func (m funcMitigator) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	return m.plan(v, b)
}

// A mitigator that names its object by list edits (no raw patch) is a valid
// plan. The executor's plan builder must hand it on unchanged: the stage applies
// exactly what the builder returns.
func TestMitigationPlanBuilderKeepsListEditPlansApplicable(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	m := funcMitigator{id: "list-edit", plan: func(program.ReadView, program.Bindings) (program.Plan, error) {
		edit := program.ListEdit{Path: "/spec/template/spec/containers", Match: map[string]any{"name": "sidecar"}, Op: "remove"}
		return program.Plan{Actions: []program.Action{program.PatchObject{Ref: ref, Edits: []program.ListEdit{edit}, Reason: "remove the sidecar"}}}, nil
	}}
	plan, err := BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("builder returned a plan the executor rejects: %v", err)
	}
	if _, err := generalActionProto(plan.Actions[0]); err != nil {
		t.Fatalf("builder returned a plan the executor cannot encode: %v", err)
	}
}

// The builder must show the mitigator the same facts the diagnosis saw, not a
// lossy copy: pod admission and generic objects are read by learned programs.
func TestMitigationPlanBuilderViewCarriesAdmissionAndObjects(t *testing.T) {
	quota := sdk.ObjectRef{APIVersion: "v1", Kind: "ResourceQuota", Namespace: "app", Name: "tight"}
	snapshot := sdktest.Snapshot{
		NamespaceName: "app",
		Admission:     &sdk.PodAdmissionResult{Namespace: "app", Rejection: "failed quota: tight"},
		Objects:       []unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": map[string]any{"name": "tight", "namespace": "app"}}}},
	}
	m := funcMitigator{id: "quota", plan: func(v program.ReadView, _ program.Bindings) (program.Plan, error) {
		if _, ok := v.PodAdmission(); !ok {
			return program.Plan{}, nil
		}
		if _, err := v.Object(quota); err != nil {
			return program.Plan{}, err
		}
		return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: quota, Reason: "remove the quota named by admission"}}}, nil
	}}
	plan, err := BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: snapshot}, program.Bindings{}, time.Second)
	if err != nil {
		t.Fatalf("mitigator saw a different view than the one it was given: %v", err)
	}
	if len(plan.Actions) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestMitigationPlanBuilderRejectsEmptyPlan(t *testing.T) {
	m := funcMitigator{id: "empty", plan: func(program.ReadView, program.Bindings) (program.Plan, error) { return program.Plan{}, nil }}
	_, err := BuildMitigationPlan(context.Background(), m, &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, program.Bindings{}, time.Second)
	if err == nil {
		t.Fatal("an empty plan is not a mitigation")
	}
}
