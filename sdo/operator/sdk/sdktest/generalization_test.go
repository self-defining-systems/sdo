package sdktest

import (
	"context"
	"encoding/json"
	"sdo.dev/operator/sdk/program"
	"testing"
)

type sourcePhaseProgram struct{}

func (sourcePhaseProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "phase", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (sourcePhaseProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b["service"])
	if err != nil {
		return program.Verdict{}, err
	}
	if diff.Changed {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestGeneralizationUsesDistinctRecordedPhases(t *testing.T) {
	c := renameFixture(t)
	args, _ := json.Marshal([]any{c.Bindings["service"]})
	peer, _ := json.Marshal([]any{c.PeerBindings["service"]})
	// JSON decoding keeps this test executable before phase metadata is added.
	raw := []byte(`[{"phase":"open","method":"DiffAgainstSource","arguments":` + string(args) + `,"result":{"changed":true}},{"phase":"fixed","method":"DiffAgainstSource","arguments":` + string(args) + `,"result":{"changed":false}},{"phase":"healthy","method":"DiffAgainstSource","arguments":` + string(peer) + `,"result":{"changed":false}}]`)
	if err := json.Unmarshal(raw, &c.Recordings); err != nil {
		t.Fatal(err)
	}
	CheckGeneralization(t, sourcePhaseProgram{}, c, c.PeerBindings)
	CheckRenameInvariance(t, sourcePhaseProgram{}, c)
}
