package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A ProposeSourceChange records its resolved value and writes nothing; an
// Escalate plan records its report only. Both survive the wire form.
func TestContractActionsRecordWithoutWriting(t *testing.T) {
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	spec := program.ProgramSpec{ID: "scale-up", Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue, Required: true}}}
	reason := "web saturates at the accepted replica count under the observed load; keep the scaled count"
	plan := program.BindPlanValues(program.Plan{Actions: []program.Action{program.ProposeSourceChange{Ref: deployment, Path: "spec.replicas", Value: program.FromParam("replicas"), Reason: reason}}},
		program.ValueInputs{Spec: spec, Bindings: program.Bindings{Values: map[string]any{"replicas": 3}}})
	incident := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}
	result, err := (PlanExecutor{}).Apply(context.Background(), incident, plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Patches) != 0 || len(result.ActionReports) != 1 || result.ActionReports[0].Status != "applied" {
		t.Fatalf("result = %+v", result)
	}
	change := result.ActionReports[0].SourceChange
	if change == nil || change.Path != "spec.replicas" || change.Value != 3 || change.Ref != deployment || change.Reason != reason {
		t.Fatalf("proposal = %+v", change)
	}
	application, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(application).Result
	if got := decoded.ActionReports[0].SourceChange; got == nil || got.Path != "spec.replicas" {
		t.Fatalf("decoded proposal = %+v", got)
	}
	if _, ok := program.SourceChangeAction(decoded.Plan.Actions[0]); !ok {
		t.Fatalf("decoded plan = %+v", decoded.Plan)
	}

	escalate := program.Plan{Actions: []program.Action{program.Escalate{Reason: "the dependency is slow outside the namespace", Resources: []sdk.ObjectRef{deployment}}}}
	result, err = (PlanExecutor{}).Apply(context.Background(), incident, escalate, false)
	if err != nil || len(result.Patches) != 0 || result.ActionReports[0].Status != "applied" || result.Applied {
		t.Fatalf("escalate result = %+v, %v", result, err)
	}
	outside := program.Plan{Actions: []program.Action{program.ProposeSourceChange{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "web"}, Path: "spec.replicas", Value: program.FromParam("replicas"), Reason: reason}}}
	if _, err := (PlanExecutor{}).Apply(context.Background(), incident, outside, false); err == nil {
		t.Fatal("a proposal outside the incident namespace was recorded")
	}
}
