package lifecyclecore

import (
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestUnconfirmedKeepsClosurePendingAndUnverified(t *testing.T) {
	final := []*contracts.DetectorEvaluation{{DetectorId: "service-reachability", Status: "clear"}}
	state := &contracts.ControllerState{
		ClosureState:   contracts.ClosureState_CLOSURE_STATE_PENDING,
		PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "incident-1"}, FinalDetectorStates: final},
	}
	next, effects := Step(state, Event{Kind: Unconfirmed, At: time.Unix(100, 0), Error: "service-reachability: observation window is incomplete"})
	if len(effects) != 0 || next.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_PENDING || next.GetClosureFailure() != nil || len(next.GetFailedClosures()) != 0 {
		t.Fatalf("unconfirmed must stay pending without failure: %v %v", next, effects)
	}
	closure := next.GetPendingClosure()
	if len(closure.GetFinalDetectorStates()) != 0 || len(closure.GetPartialDetectorStates()) != 1 ||
		closure.GetUnconfirmedReason() != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE ||
		closure.GetUnconfirmedDetail() == "" {
		t.Fatalf("closure = %v", closure)
	}
	if len(state.GetPendingClosure().GetFinalDetectorStates()) != 1 {
		t.Fatal("input state was mutated")
	}
	if _, processing := Step(next, Event{Kind: Drive, At: time.Unix(101, 0)}); len(processing) != 1 || len(processing[0].Closure.GetFinalDetectorStates()) != 0 {
		t.Fatalf("unconfirmed closure must still process: %v", processing)
	}
	if again, _ := Step(next, Event{Kind: Unconfirmed, At: time.Unix(102, 0), Error: "other"}); again.GetPendingClosure().GetUnconfirmedDetail() != closure.GetUnconfirmedDetail() {
		t.Fatal("unconfirmed is applied once")
	}
}
