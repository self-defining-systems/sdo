package program

import (
	"reflect"
	"strings"
	"testing"
)

func syntheticProcedure() Procedure {
	return Procedure{
		ID: "reindex", Component: "api", Container: "app", Effect: ProcedureRepair, Source: "docs/ops.md",
		Command: []string{"shopctl", "reindex", "--scope={{scope}}", "--batch", "{{batch}}", "--dry={{dry}}"},
		Params: []ProcedureParam{
			{Name: "scope", Type: ProcedureString, Pattern: "[a-z]+", Default: "all"},
			{Name: "batch", Type: ProcedureInt, Required: true},
			{Name: "dry", Type: ProcedureBool, Default: false},
		},
	}
}

func TestProcedureBindSubstitutesTypedValues(t *testing.T) {
	argv, err := syntheticProcedure().Bind(map[string]any{"batch": 50, "dry": true})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"shopctl", "reindex", "--scope=all", "--batch", "50", "--dry=true"}; !reflect.DeepEqual(argv, want) {
		t.Fatalf("argv = %q, want %q", argv, want)
	}
	// JSON numbers arrive as float64 from bindings.
	if argv, err := syntheticProcedure().Bind(map[string]any{"batch": float64(7)}); err != nil || argv[4] != "7" {
		t.Fatalf("argv = %q, err = %v", argv, err)
	}
}

func TestProcedureBindRejectsInvalidValues(t *testing.T) {
	for name, values := range map[string]map[string]any{
		"missing required": {},
		"wrong type":       {"batch": "50"},
		"fraction":         {"batch": 1.5},
		"pattern":          {"batch": 1, "scope": "All"},
		"injection":        {"batch": 1, "scope": "a; rm -rf /"},
		"unknown":          {"batch": 1, "other": "x"},
		"bool type":        {"batch": 1, "dry": "yes"},
	} {
		if _, err := syntheticProcedure().Bind(values); err == nil {
			t.Fatalf("%s: Bind accepted %v", name, values)
		}
	}
}

func TestProcedureDefaultAlphabetIsOneInertWord(t *testing.T) {
	p := Procedure{ID: "show", Component: "api", Container: "app", Effect: ProcedureCheck, Source: "README", Command: []string{"tool", "show", "{{name}}"}, Params: []ProcedureParam{{Name: "name", Type: ProcedureString}}}
	for _, bad := range []string{"", "-rf", "a b", "a'b", "$HOME", "a\nb", strings.Repeat("x", 300)} {
		if _, err := p.Bind(map[string]any{"name": bad}); err == nil {
			t.Fatalf("Bind accepted %q", bad)
		}
	}
	if argv, err := p.Bind(map[string]any{"name": "user@example.org"}); err != nil || argv[2] != "user@example.org" {
		t.Fatalf("argv = %q, err = %v", argv, err)
	}
}

func TestJobProcedureTakesNoCommand(t *testing.T) {
	job := Procedure{ID: "migrate", Job: "Job/migrate", Effect: ProcedureData, Source: "manifest"}
	if err := job.Validate(); err != nil {
		t.Fatal(err)
	}
	job.Command = []string{"run"}
	if err := job.Validate(); err == nil {
		t.Fatal("a Job procedure with a command validated")
	}
}
