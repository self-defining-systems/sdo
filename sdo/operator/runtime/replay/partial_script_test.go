package replay

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type partialScriptFixture struct{}

func (partialScriptFixture) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "field", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (partialScriptFixture) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["service"]
	for _, service := range v.Services() {
		if service.Namespace == ref.Namespace && service.Name == ref.Name {
			result := program.NoMatch
			if service.Spec.Selector["app"] != "api" {
				result = program.Match
			}
			return program.Verdict{Result: result, Evidence: []program.Evidence{{Resource: ref, Fact: "selector", Observed: service.Spec.Selector["app"], Expected: "api"}}}, nil
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}
func (partialScriptFixture) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["service"]
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl patch service "$SERVICE" --type merge -p '{"spec":{"selector":{"app":"api"}}}'`, Env: map[string]program.Value{"SERVICE": program.FromObserved(ref, "metadata.name")}}}}, nil
}

// The same-kind peer still matches its own diagnosis in fixed. A script step
// is checked on its bound resource, through both production and SDK replay.
func TestPartialRunScriptReplayKeepsUnrelatedFaults(t *testing.T) {
	p := partialScriptFixture{}
	c := fixture("field")
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	c.Corpus.Bindings = program.Bindings{Objects: program.ObjectBindings{"service": ref}}
	peer := c.Corpus.Open.ServiceList[0].DeepCopy()
	peer.Name = "peer"
	c.Corpus.Open.ServiceList = append(c.Corpus.Open.ServiceList, *peer)
	c.Corpus.Fixed.ServiceList = append(c.Corpus.Fixed.ServiceList, *peer)
	plan, err := p.Mitigate(context.Background(), nil, c.Corpus.Bindings)
	if err != nil {
		t.Fatal(err)
	}
	script := plan.Actions[0].(program.RunScript)
	receipt := program.ScriptOriginReceipt{ScriptDigest: program.ScriptDigest(script.Script), ID: "performed-bash-step", IncidentID: c.ID, Success: true, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	c.Corpus.Recordings = []sdktest.PrimitiveCall{{Method: "RepairReceipt", Arguments: json.RawMessage("[]"), Result: raw}}
	c.Corpus.Provenance.IncidentID = c.ID
	c.Corpus.ScriptOrigins = map[string]string{program.ScriptDigest(script.Script): receipt.ID}
	for _, test := range []struct {
		name   string
		broken bool
	}{{"partial step", false}, {"step itself still broken", true}} {
		t.Run(test.name, func(t *testing.T) {
			candidate := c
			if test.broken {
				candidate.Corpus.Fixed = c.Corpus.Open
			}
			got := CheckRepair(context.Background(), p, p, candidate)
			sdkResult, sdkErr := sdktest.CheckMitigation(p, p, candidate.Corpus)
			if test.broken {
				if got.Status == "replayed" || sdkErr == nil {
					t.Fatalf("broken step accepted: %+v; %v", got, sdkErr)
				}
				return
			}
			if got.Status != "replayed" || got.ReplayBasis != "observed-transition" || got.Post.Result != program.NoMatch {
				t.Fatalf("partial script rejected: %+v", got)
			}
			if sdkErr != nil || sdkResult.Status != sdktest.MitigationReplayObservedTransition {
				t.Fatalf("SDK rejected partial script: %+v %v", sdkResult, sdkErr)
			}
			peerCase := candidate
			peerCase.Corpus.Bindings = program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}}}
			peerVerdict := program.RunDiagnose(context.Background(), p, &sdktest.ReplayReadView{Snapshot: candidate.Corpus.Fixed}, peerCase.Corpus.Bindings, time.Second)
			if peerVerdict.Result != program.Match {
				t.Fatalf("fixture lost the unrelated fault: %+v", peerVerdict)
			}
		})
	}
}
