package replay

import (
	"context"
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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

func TestSecretRestoreUsesObservedTransitionInRepairReplay(t *testing.T) {
	args, _ := json.Marshal([]program.SecretKey{
		{Ref: replaySecret, Key: "key"}, {Ref: replaySecret, Key: "key", Accepted: true},
	})
	p := secretRestoreProgram{}
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	c := Case{Corpus: sdktest.CorpusCase{
		Open: snapshot, Fixed: snapshot, Healthy: snapshot,
		RootCauseProgramIDs: []string{p.Spec().ID},
		Recordings: []sdktest.PrimitiveCall{
			{Phase: "open", Method: "SecretEqual", Arguments: args, Result: json.RawMessage("false")},
			{Phase: "fixed", Method: "SecretEqual", Arguments: args, Result: json.RawMessage("true")},
		},
	}}
	if got := CheckRepair(context.Background(), p, p, c); got.Status != "replayed" || got.ReplayBasis != "observed-transition" {
		t.Fatalf("repair replay = %s (%s): %v", got.Status, got.ReplayBasis, got.Errors)
	}
	c.Corpus.Recordings[1].Result = json.RawMessage("false")
	if got := CheckRepair(context.Background(), p, p, c); got.Status == "replayed" {
		t.Fatal("replay accepted a fixed observation that still diagnoses the fault")
	}
}
