package replay

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var planValidationQuery = program.Query{Store: &program.StoreQuery{
	Target: program.StoreTarget{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"},
	Scope:  program.DataScope{Database: "app", Collection: "items", Selector: "status = 2", Limit: 32},
	Fields: []string{"status"},
}}

type computedRepair struct{ prefix string }

func (computedRepair) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fix-status"} }
func (computedRepair) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	observation, err := view.Read(planValidationQuery)
	if err != nil {
		return program.Verdict{}, err
	}
	if observation.Count > 0 {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p computedRepair) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	if _, err := view.Read(planValidationQuery); err != nil {
		return program.Plan{}, err
	}
	var assignments []string
	for _, field := range planValidationQuery.Store.Fields {
		assignments = append(assignments, field+" = 0")
	}
	return program.Plan{Actions: []program.Action{program.RepairData{
		Target: planValidationQuery.Store.Target, Scope: planValidationQuery.Store.Scope,
		Statement: p.prefix + strings.Join(assignments, ", "), ExpectMin: 1, ExpectMax: 32,
	}}}, nil
}

func TestCapturedPlanValidationUsesComputedPlanAndExactRecordedRead(t *testing.T) {
	args, err := json.Marshal([]program.Query{planValidationQuery})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, prefix, wantError string
		recorded                bool
	}{
		{name: "missing read", wantError: "status = 2"},
		{name: "recorded invalid plan", recorded: true, wantError: "SET clause"},
		{name: "recorded valid plan", recorded: true, prefix: "SET "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Case{ID: "captured", Corpus: sdktest.CorpusCase{
				Open: sdktest.Snapshot{NamespaceName: "app"}, RootCauseProgramIDs: []string{"fix-status"},
			}}
			if tc.recorded {
				c.Corpus.Open.Readings = []sdktest.PrimitiveCall{{
					Method: "Read", Arguments: args, Result: json.RawMessage(`{"count":1,"rows":[{"status":2}]}`),
				}}
			}
			p := computedRepair{prefix: tc.prefix}
			err := ValidateCapturedPlan(context.Background(), p, p, c)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("validation error = %v, want %q", err, tc.wantError)
			}
		})
	}
}
