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
		{"bare assignment", `"status = 0"`, "1", "32", ""},
		{"whole statement", `"UPDATE packages"`, "1", "32", "no store engine"},
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

// A statement or scope the program states in constants is judged whole, as the engine it can only be for will run it:
// a program accepted here is not refused at run time for its own text.
func TestStoreTextInConstantsIsJudgedAsTheEngineWillRunIt(t *testing.T) {
	const head = "package p\n\nimport (\n\t\"strings\"\n\n\t\"sdo.dev/operator/sdk/program\"\n\t\"sdo.dev/operator/sdk/storeops\"\n)\n\nconst (\n\ttable = \"tokens\"\n\tvalue = \"---\\n- :api\\n\"\n\tbad = \"a = 'x' --\"\n)\n\nvar ids []string\n\n"
	for name, tc := range map[string]struct{ body, want string }{
		"constant value with hazards": {"var a = program.RepairData{Scope: storeops.Scope{Collection: table, Selector: \"id IN (\" + strings.Join(ids, \",\") + \")\"}, Statement: \"scopes = '\" + value + \"'\"}", ""},
		"constant comment":            {"var a = program.RepairData{Statement: bad}", "--"},
		"concatenated comment":        {"func f() program.Action {\n\tconst tail = \" -- x\"\n\treturn program.RepairData{Statement: \"a = 1\" + tail}\n}", "--"},
		"selector leaves its scope":   {"var a = program.RepairData{Scope: storeops.Scope{Collection: table, Selector: \"TRUE) OR (TRUE\"}, Statement: \"delete\"}", "parenthesis"},
		"redis command on a table":    {"var a = program.RepairData{Scope: program.DataScope{Collection: table}, Statement: \"SREM x\"}", "no store engine"},
		"named engine":                {"var a = program.RepairData{Target: program.StoreTarget{Engine: \"postgres\"}, Statement: \"SREM x\"}", "SET clause"},
		"scope alone":                 {"var s = storeops.Scope{Collection: table, Selector: bad}", "--"},
		"negative limit":              {"var s = storeops.Scope{Collection: table, Limit: -1}", "limit"},
		"computed statement":          {"func f(s string) program.Action { return program.RepairData{Statement: \"a = '\" + s + \"' --\"} }", ""},
		"read selector":               {"var q = program.StoreQuery{Scope: storeops.Scope{Collection: table, Selector: \"lower(a) = 'x'\"}, Fields: []string{\"id\"}}", "subquery"},
		"read field":                  {"var q = program.StoreQuery{Scope: storeops.Scope{Collection: table}, Fields: []string{\"a b\"}}", "a b"},
		"good read":                   {"var q = &program.StoreQuery{Scope: storeops.Scope{Database: \"d\", Collection: table, Selector: \"a = '\" + value + \"'\"}, Fields: []string{\"id\", table}}", ""},
		"scope filled later":          {"func f() program.DataScope {\n\ts := program.DataScope{Database: \"app\"}\n\ts.Collection = table\n\treturn s\n}", ""},
		"database alone in a repair":  {"var a = program.RepairData{Scope: program.DataScope{Database: \"app\"}, Statement: \"a = 1\"}", ""},
		"selector before its table":   {"var s = program.DataScope{Selector: \"a = 1\"}", ""},
		"query filled later":          {"func f() program.StoreQuery {\n\tq := program.StoreQuery{Scope: program.DataScope{Database: \"app\"}}\n\tq.Fields = ids\n\treturn q\n}", ""},
		"empty literals":              {"var a, q, s = program.RepairData{}, program.StoreQuery{Scope: program.DataScope{}}, program.DataScope{}", ""},
		"computed read":               {"func q(s storeops.Scope, f []string) program.StoreQuery { return program.StoreQuery{Scope: s, Fields: f} }", ""},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := ".sdo/playbooks/fix/mitigate.go"
			write(t, root, path, head+tc.body+"\n")
			ds := repairStatementDiagnostics(root, []string{path})
			if tc.want == "" && len(ds) != 0 || tc.want != "" && (len(ds) != 1 || !strings.Contains(ds[0].Message, tc.want)) {
				t.Fatalf("diagnostics = %+v, want %q", ds, tc.want)
			}
		})
	}
}

// A constant of the package's other file is the statement's own.
func TestStoreTextReadsThePackagesOtherFiles(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".sdo/playbooks/fix/diagnose.go", "package p\n\nconst statement = \"a = 1 --\"\n")
	path := ".sdo/playbooks/fix/mitigate.go"
	write(t, root, path, fmt.Sprintf(repairSource, "statement", "1", "1"))
	if ds := repairStatementDiagnostics(root, []string{path}); len(ds) != 1 || ds[0].Path != path {
		t.Fatalf("diagnostics = %+v", ds)
	}
}
