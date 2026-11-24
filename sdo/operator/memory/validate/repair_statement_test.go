package validate

import (
	"fmt"
	"strings"
	"testing"
)

const repairSource = "package p\n\nimport \"sdo.dev/operator/sdk/program\"\n\nfunc plan(target program.StoreTarget, scope program.DataScope) program.Plan {\n\treturn program.Plan{Actions: []program.Action{program.RepairData{Target: target, Scope: scope, Statement: %s, ExpectMin: %s, ExpectMax: %s}}}\n}\n"

// A learned Mitigate must never fail on apply for a reason the check could have caught: the statement of a
// RepairData is checked against the store engines exactly as the engine will apply it.
func TestRepairStatementLiteralsAreCheckedAsTheEngineWillApplyThem(t *testing.T) {
	for _, tc := range []struct {
		name, statement, min, max string
		want                      string
	}{
		{"postgres SET clause", `"SET status = 0"`, "1", "32", ""},
		{"delete", `"delete"`, "1", "32", ""},
		{"redis command", `"SREM member"`, "1", "32", ""},
		{"mongodb update document", "`{\"$set\": {\"a\": 1}}`", "1", "32", ""},
		{"bare assignment that no engine applies", `"status = 0"`, "1", "32", "no store engine"},
		{"expectation that changes nothing", `"delete"`, "0", "0", "ExpectMax"},
		{"inverted expectation", `"delete"`, "5", "2", "ExpectMin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := ".sdo/playbooks/fix/mitigate.go"
			write(t, root, path, fmt.Sprintf(repairSource, tc.statement, tc.min, tc.max))
			ds := repairStatementDiagnostics(root, []string{path})
			if tc.want == "" {
				if len(ds) != 0 {
					t.Fatalf("diagnostics = %v", ds)
				}
				return
			}
			if len(ds) != 1 || ds[0].Rule != RuleProgramPlanInvalid || !strings.Contains(ds[0].Message, tc.want) || ds[0].Path != path {
				t.Fatalf("diagnostics = %+v, want one %s mentioning %q", ds, RuleProgramPlanInvalid, tc.want)
			}
		})
	}
}

// Publication must catch the raw statement even without a captured Store Read
// that would let Mitigate reach plan construction.
func TestCheckValidatesRawRepairStatementsWithoutReplay(t *testing.T) {
	const id = "fix-store-policy"
	const path = ".sdo/playbooks/" + id + "/mitigate.go"
	const diagnose = `package p

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "fix-store-policy"}
}

func (Program) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Unknown}, nil
}
`
	const mitigate = `package p

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	pods := view.Pods()
	if len(pods) == 0 {
		return program.Plan{}, nil
	}
	pod := pods[0]
	target := program.StoreTarget{Component: pod.Labels["app"], Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace}, Container: "db"}
	scope := program.DataScope{Collection: "items", Selector: "status = 2", Limit: 32}
	if _, err := view.Read(program.Query{Store: &program.StoreQuery{Target: target, Scope: scope, Fields: []string{"status"}}}); err != nil {
		return program.Plan{}, err
	}
	%s
	return program.Plan{Actions: []program.Action{program.RepairData{Target: target, Scope: scope, Statement: %s, ExpectMin: 1, ExpectMax: 32}}}, nil
}
`
	const loop = `statement := ""
	for _, column := range []string{"status", "enabled"} {
		if statement != "" {
			statement += ", "
		}
		statement += column + " = 0"
	}`
	for _, tc := range []struct {
		name, build, statement string
		rules                  []string
	}{
		{"loop-built assignment", loop, "statement", []string{RuleProgramPlanInvalid}},
		{"literal SET clause", "", `"SET status = 0, enabled = 0"`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := fmt.Sprintf(mitigate, tc.build, tc.statement)
			row := gateComponentLiteral(tc.name, false, []string{id}, writePlaybook(id, diagnose, source), tc.rules...)
			if len(tc.rules) > 0 {
				row.path = path
			}
			request := row.request
			row.request = func(r *Request) {
				request(r)
				r.Replay = "off"
			}
			runGateRow(t, row)
		})
	}
}
