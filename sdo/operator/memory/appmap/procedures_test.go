package appmap

import (
	"strings"
	"testing"
	"time"
)

const shopProcedures = `procedures:
  - id: self-check
    component: api
    container: app
    command: [shopctl, check, "--scope={{scope}}"]
    params:
      - {name: scope, type: string, pattern: "[a-z]+", default: all}
    effect: check
    verify: exits 0
    timeout: 2m
    source: docs/operations.md
  - id: reindex
    component: api
    container: app
    command: [shopctl, reindex, "--batch", "{{batch}}"]
    params:
      - {name: batch, type: int, required: true}
    effect: repair
    source: docs/operations.md
  - id: migrate
    job: Job/migrate
    effect: data
    source: "manifest: Job/migrate"
`

const procedureInventory = shopInventory + "- Job/shop/migrate (source: k8s/migrate.yaml)\n"

func procedureFindings(t *testing.T, procedures string) []Finding {
	t.Helper()
	var out []Finding
	for _, f := range mustParse(t, shopYAML+procedures).Check(ParseInventory(procedureInventory), shopWorkloads) {
		if f.Rule != RuleUnexercisedEdge {
			out = append(out, f)
		}
	}
	return out
}

func TestProceduresParseAndCheck(t *testing.T) {
	if findings := procedureFindings(t, shopProcedures); len(findings) != 0 {
		t.Fatalf("findings = %+v", findings)
	}
	m := mustParse(t, shopYAML+shopProcedures)
	procedures := m.ProgramProcedures()
	if len(procedures) != 3 || procedures[0].Timeout != 2*time.Minute || procedures[0].Params[0].Default != "all" || procedures[2].Job != "Job/migrate" {
		t.Fatalf("procedures = %+v", procedures)
	}
}

// A map without procedures stays valid: the section is optional.
func TestProceduresAreOptional(t *testing.T) {
	if findings := procedureFindings(t, ""); len(findings) != 0 {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestProcedureRulesNameTheFault(t *testing.T) {
	for name, tc := range map[string]struct{ from, to, rule, mention string }{
		"unknown effect":      {"effect: check", "effect: maybe", RuleSchema, `effect "maybe"`},
		"no source":           {"    source: docs/operations.md\n  - id: reindex", "  - id: reindex", RuleSchema, "has no source"},
		"unknown component":   {"component: api\n    container: app\n    command: [shopctl, check", "component: nope\n    container: app\n    command: [shopctl, check", RuleUnknownName, `component "nope"`},
		"job not inventoried": {"job: Job/migrate", "job: Job/absent", RuleUnknownName, `job "Job/absent"`},
		"undeclared param":    {`"--scope={{scope}}"`, `"--scope={{scope}}", "{{other}}"`, RuleSchema, "{{other}}"},
		"unused param":        {`"--scope={{scope}}"`, `"--all"`, RuleSchema, "appears in no command argument"},
		"bad default":         {"default: all", "default: ALL", RuleSchema, "default"},
		"bad timeout":         {"timeout: 2m", "timeout: soon", RuleSchema, "timeout"},
		"too long":            {"timeout: 2m", "timeout: 1h", RuleSchema, "timeout"},
		"job with command":    {"job: Job/migrate", "job: Job/migrate\n    command: [run]", RuleSchema, "takes no container"},
		"both forms":          {"job: Job/migrate", "job: Job/migrate\n    component: api", RuleSchema, "either in a component"},
		"no container":        {"    container: app\n    command: [shopctl, check", "    command: [shopctl, check", RuleSchema, "names no container"},
		"duplicate":           {"id: reindex", "id: self-check", RuleSchema, "more than once"},
	} {
		t.Run(name, func(t *testing.T) {
			text := strings.Replace(shopProcedures, tc.from, tc.to, 1)
			if text == shopProcedures {
				t.Fatalf("fixture edit %q did not apply", tc.from)
			}
			findings := procedureFindings(t, text)
			if !has(findings, tc.rule, tc.mention) {
				t.Fatalf("findings = %+v, want %s mentioning %q", findings, tc.rule, tc.mention)
			}
		})
	}
}

func TestProcedureUnknownFieldIsRejected(t *testing.T) {
	if _, err := Parse(shopYAML + strings.Replace(shopProcedures, "effect: data", "effect: data\n    safe: true", 1)); err == nil {
		t.Fatal("an unknown procedure field parsed")
	}
}
