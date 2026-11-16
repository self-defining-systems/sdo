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
