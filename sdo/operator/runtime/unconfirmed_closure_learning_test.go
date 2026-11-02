package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
)

// A verified repair whose closure recheck window never completed commits an
// unconfirmed outcome, never a failed one. Reflection still runs on it.
func TestUnconfirmedClosureCommitsOutcomeWithoutFailureLearning(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, workspace IncidentWorkspace, outcome *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			calls++
			if outcome.GetClassification() != outcomeclass.Unconfirmed {
				t.Errorf("reflection saw classification %q", outcome.GetClassification())
			}
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "closure window incomplete; repair evidence is unconfirmed"}, nil
		}),
	}
	workspace, err := native.PrepareIncident(ctx, "unconfirmed-window")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Result.ResponderSessionID = "responder-unconfirmed"
	closure.PartialDetectorStates, closure.FinalDetectorStates = closure.FinalDetectorStates, nil
	closure.UnconfirmedReason = contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE
	closure.UnconfirmedDetail = "closure observation detector health is unconfirmed: observation window is incomplete"
	// Unconfirmed is non-success: its reflection runs queued behind the ACK.
	receipt, err := closeAndReflect(t, ctx, native, closure)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OutcomeCommit == "" || ledger.GetOutcomeDocument().GetClassification() != outcomeclass.Unconfirmed {
		t.Fatalf("unconfirmed outcome not committed: receipt=%+v ledger=%v", receipt, ledger)
	}
	decision := ledger.GetReflectionDecision()
	if calls != 1 || !ledger.ReflectionProcessed || decision.GetLearningDecision() != "no_change" || decision.GetError() != "" {
		t.Fatalf("unconfirmed closure reflection was not accepted as a no-op: calls=%d decision=%v", calls, decision)
	}
}
