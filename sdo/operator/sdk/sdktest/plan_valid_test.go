package sdktest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type planMitigator struct {
	plan program.Plan
	err  error
}

func (planMitigator) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fix"} }

func (m planMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return m.plan, m.err
}

func repairPlan(statement string) program.Plan {
	target := program.StoreTarget{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"}
	scope := program.DataScope{Database: "app", Collection: "packages", Selector: "status = 2", Limit: 32}
	return program.Plan{Actions: []program.Action{program.RepairData{Target: target, Scope: scope, Statement: statement, ExpectMin: 1, ExpectMax: 32}}}
}

// A data-writing plan is checked as the host will apply it, so a learned program cannot fail on apply for a reason
// the check could have caught.
func TestCheckPlanValidRefusesAPlanTheHostWillRefuse(t *testing.T) {
	if err := CheckPlanValid(planMitigator{plan: repairPlan("SET status = 0")}, CorpusCase{}); err != nil {
		t.Fatalf("valid plan: %v", err)
	}
	err := CheckPlanValid(planMitigator{plan: repairPlan("status = 0")}, CorpusCase{})
	if err == nil || !strings.Contains(err.Error(), "program.plan-invalid") || !strings.Contains(err.Error(), "SET clause") {
		t.Fatalf("invalid statement: %v", err)
	}
	if err := CheckPlanValid(planMitigator{err: errors.New("no store in the recorded reads")}, CorpusCase{}); err != nil {
		t.Fatalf("a Mitigate that cannot run is unverified, not invalid: %v", err)
	}
}
