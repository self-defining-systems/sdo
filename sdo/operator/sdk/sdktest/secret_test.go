package sdktest

import (
	"context"
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var replaySecret = sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}

type secretRestoreProgram struct{}

func (secretRestoreProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "restore"} }
func (secretRestoreProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	equal, err := v.SecretEqual(program.SecretKey{Ref: replaySecret, Key: "key"}, program.SecretKey{Ref: replaySecret, Key: "key", Accepted: true})
	if err != nil {
		return program.Verdict{}, err
	}
	if equal {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (secretRestoreProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: replaySecret}}}, nil
}

func TestSecretRestoreReplaysValueFreeObservedTransition(t *testing.T) {
	args, _ := json.Marshal([]program.SecretKey{
		{Ref: replaySecret, Key: "key"}, {Ref: replaySecret, Key: "key", Accepted: true},
	})
	p := secretRestoreProgram{}
	c := CorpusCase{Recordings: []PrimitiveCall{
		{Phase: "open", Method: "SecretEqual", Arguments: args, Result: json.RawMessage("false")},
		{Phase: "fixed", Method: "SecretEqual", Arguments: args, Result: json.RawMessage("true")},
	}}
	got, err := CheckMitigation(p, p, c)
	if err != nil || got.Status != MitigationReplayObservedTransition {
		t.Fatalf("CheckMitigation = %+v, %v", got, err)
	}
	c.Recordings[1].Result = json.RawMessage("false")
	if _, err := CheckMitigation(p, p, c); err == nil {
		t.Fatal("accepted a recording that still diagnoses the fault")
	}
}
