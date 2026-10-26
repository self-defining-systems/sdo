package replay

import (
	"context"
	"encoding/json"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type scriptedFixture struct{ fixtureProgram }

func (p scriptedFixture) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl exec "$POD" -- bash -s`, Env: map[string]program.Value{"POD": program.FromParam("target")}}}}, nil
}
func (p scriptedFixture) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "field", Params: []program.ParamSpec{{Name: "target", ValueKind: program.StringValue, Required: true}}}
}

func TestRunScriptObservedTransitionDoesNotInventActionResults(t *testing.T) {
	p := scriptedFixture{fixtureProgram{mode: "field"}}
	c := fixture("field")
	c.Corpus.Bindings = program.Bindings{Values: map[string]any{"target": "current-related-pod"}}
	plan, _ := p.Mitigate(context.Background(), nil, c.Corpus.Bindings)
	script := plan.Actions[0].(program.RunScript)
	receipt := program.ScriptOriginReceipt{ID: "manual-exec", IncidentID: c.ID, Success: true, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)}
	raw, _ := json.Marshal(receipt)
	c.Corpus.Recordings = append(c.Corpus.Recordings, sdktest.PrimitiveCall{Method: "RepairReceipt", Arguments: json.RawMessage("[]"), Result: raw})
	c.Corpus.Provenance.IncidentID = c.ID
	c.Corpus.ScriptOrigins = map[string]string{program.ScriptDigest(script.Script): receipt.ID}
	r := CheckRepair(context.Background(), p, p, c)
	if r.Status != "replayed" || r.ReplayBasis != "observed-transition" || r.Post.Result != program.NoMatch || len(r.Actions) != 0 {
		t.Fatalf("opaque transition rejected or simulated: %+v", r)
	}
	for name, mutate := range map[string]func(*Case){
		"no fixed clearance": func(c *Case) { c.Corpus.Fixed = c.Corpus.Open },
		"no receipt":         func(c *Case) { c.Corpus.Recordings = nil },
		"failed receipt": func(c *Case) {
			receipt.Success = false
			raw, _ := json.Marshal(receipt)
			c.Corpus.Recordings = []sdktest.PrimitiveCall{{Method: "RepairReceipt", Result: raw}}
		},
		"no linkage":    func(c *Case) { c.Corpus.ScriptOrigins = nil },
		"unbound input": func(c *Case) { c.Corpus.Bindings = program.Bindings{} },
	} {
		t.Run(name, func(t *testing.T) {
			broken := c
			broken.Corpus.Recordings = append([]sdktest.PrimitiveCall(nil), c.Corpus.Recordings...)
			mutate(&broken)
			r := CheckRepair(context.Background(), p, p, broken)
			if r.Status == "replayed" {
				t.Fatalf("invalid observed transition accepted: %+v", r)
			}
		})
	}
}
