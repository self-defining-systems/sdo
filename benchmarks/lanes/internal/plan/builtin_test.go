package plan

import (
	"encoding/json"
	"strings"
	"testing"
)

const builtinPlan = `
plan: tp
pins: {workload: fuzzops@4652656}
arms: [{name: a, spec: tpl-builtin.json}]
items: [x]
`

// A builtin (hookless) operator has no revision: the plan needs no subject pin and the rendered spec carries none.
func TestBuiltinOperatorPlanNeedsNoSubjectPin(t *testing.T) {
	l := newLab(t)
	l.write("tpl-builtin.json", `{"app": "examples/shop", "operator": {"builtin": "noop"}, "faults": {"ids": ["${item}"]}}`)
	e := l.add(builtinPlan) // fatal when the plan is refused
	l.tick(e)
	var spec map[string]any
	if err := json.Unmarshal(mustRead(t, l.fb.subs[0].SpecPath), &spec); err != nil {
		t.Fatal(err)
	}
	op := spec["operator"].(map[string]any)
	if _, has := op["revision"]; has || op["builtin"] != "noop" || spec["fuzzops"] != "4652656" {
		t.Fatalf("rendered operator %v", op)
	}
	s, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	s.Render(&out)
	if strings.Contains(out.String(), "subject @") {
		t.Fatalf("status shows an empty pin:\n%s", out.String())
	}
}

func TestMissingWorkloadPinIsStillRefused(t *testing.T) {
	if _, err := Parse([]byte("plan: tp\npins: {subject: s@1234567}\narms: [{name: a, spec: t}]\nitems: [x]\n")); err == nil {
		t.Fatal("a plan without a workload pin was accepted")
	}
}
