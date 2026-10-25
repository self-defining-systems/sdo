package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestHealthSupersessionArchivesEveryPendingPhase(t *testing.T) {
	for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING} {
		state := &contracts.ControllerState{ClosureState: phase, PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "old"}}, ClosureReceipt: &contracts.ClosureReceipt{IncidentId: "old", OutcomeCommit: "old", AckToken: "old"}}
		before := proto.Clone(state)
		next, effects := Step(state, Event{Kind: Superseded, At: time.Unix(1000, 0), Error: "obsolete health inputs"})
		if !proto.Equal(state, before) || len(effects) != 0 || next.ClosureReceipt != nil || !next.GetClosureFailure().GetPermanent() || len(next.FailedClosures) != 1 || BlocksNext(next.ClosureState, true) {
			t.Fatalf("supersession not terminal from %v: %v", phase, next)
		}
		again, _ := Step(next, Event{Kind: Superseded, At: time.Unix(1001, 0)})
		if !proto.Equal(next, again) {
			t.Fatal("terminal archive duplicated")
		}
	}
}
