package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestGenericPlanCanRepairExactlyReferencedForeignObject(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "example.test/v1", Kind: "Policy", Name: "cluster"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"mode": "broken"})}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "repair", ValueKind: program.StringValue, Required: true}}}, Bindings: program.Bindings{Values: map[string]any{"repair": "fixed"}}}}
	plan := program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "/spec/mode", Value: program.FromParam("repair")}}}
	incident := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{PrimaryResource: ref}}}
	if _, err := executor.Apply(context.Background(), incident, plan, true); err != nil {
		t.Fatal(err)
	}
	incident.Findings = nil
	if _, err := executor.Apply(context.Background(), incident, plan, true); err == nil {
		t.Fatal("unreferenced cluster object admitted")
	}
}
