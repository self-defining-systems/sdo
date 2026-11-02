package program_test

import (
	"encoding/json"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestRunScriptContract(t *testing.T) {
	valid := program.RunScript{Script: `kubectl exec "$POD" -- bash -s < "$SCRIPT"`, Env: map[string]program.Value{"POD": program.FromParam("pod")}, Timeout: time.Minute}
	for name, mutate := range map[string]func(*program.RunScript){
		"empty":         func(a *program.RunScript) { a.Script = "" },
		"oversize":      func(a *program.RunScript) { a.Script = strings.Repeat("x", 65537) },
		"timeout":       func(a *program.RunScript) { a.Timeout = 5*time.Minute + 1 },
		"negative":      func(a *program.RunScript) { a.Timeout = -1 },
		"literal value": func(a *program.RunScript) { a.Env["POD"] = program.Value{} },
		"invalid env":   func(a *program.RunScript) { a.Env["bad-name"] = program.FromParam("pod") },
		"reserved env":  func(a *program.RunScript) { a.Env["KUBECONFIG"] = program.FromParam("pod") },
	} {
		t.Run(name, func(t *testing.T) {
			a := valid
			a.Env = map[string]program.Value{"POD": program.FromParam("pod")}
			mutate(&a)
			if (program.Plan{Actions: []program.Action{a}}).Validate() == nil {
				t.Fatal("invalid script accepted")
			}
		})
	}
	if err := (program.Plan{Actions: []program.Action{valid}}).Validate(); err != nil {
		t.Fatal(err)
	}
	plan := program.BindPlanValues(program.Plan{Actions: []program.Action{valid}}, program.ValueInputs{Spec: program.ProgramSpec{ID: "repair", Params: []program.ParamSpec{{Name: "pod", ValueKind: program.StringValue, Required: true}}}, Bindings: program.Bindings{Values: map[string]any{"pod": "$(touch /tmp/not-code)"}}})
	got, _, err := program.ResolveValue(plan.Actions[0].(program.RunScript).Env["POD"], program.ValueInputs{})
	if err != nil || got != "$(touch /tmp/not-code)" {
		t.Fatalf("bound value: %v %v", got, err)
	}
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var restored program.RunScript
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Script != valid.Script || restored.Env["POD"].Param() != "pod" {
		t.Fatal("lost inline script or provenance")
	}
}
