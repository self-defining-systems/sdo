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
	next, effects := Step(state, Event{Kind: Unconfirmed, At: time.Unix(100, 0), Error: "service-reachability: observation window is incomplete", UnconfirmedReason: contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE})
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
	if again, _ := Step(next, Event{Kind: Unconfirmed, At: time.Unix(102, 0), Error: "other", UnconfirmedReason: contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE}); again.GetPendingClosure().GetUnconfirmedDetail() != closure.GetUnconfirmedDetail() {
		t.Fatal("unconfirmed is applied once")
	}
}

func TestUnconfirmedRequiresTypedReason(t *testing.T) {
	state := &contracts.ControllerState{
		ClosureState:   contracts.ClosureState_CLOSURE_STATE_PENDING,
		PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "incident-1"}, FinalDetectorStates: []*contracts.DetectorEvaluation{{DetectorId: "health", Status: "clear"}}},
	}
	if next, _ := Step(state, Event{Kind: Unconfirmed, At: time.Unix(100, 0), Error: "untyped"}); len(next.GetPendingClosure().GetFinalDetectorStates()) != 1 {
		t.Fatal("an untyped unconfirmed event changed the closure")
	}
}

func TestObserverDeniedFailsWithTypedReasonNotSuperseded(t *testing.T) {
	state := &contracts.ControllerState{
		ClosureState:   contracts.ClosureState_CLOSURE_STATE_PENDING,
		PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "incident-1"}},
	}
	next, _ := Step(state, Event{Kind: ObserverDenied, At: time.Unix(100, 0), Error: "observation dependency unavailable: v1/Service/foreign"})
	failure := next.GetClosureFailure()
	if next.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_FAILED || !failure.GetPermanent() ||
		failure.GetReason() != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_OBSERVER_DENIED || len(next.GetFailedClosures()) != 1 {
		t.Fatalf("denied observer = %v", next)
	}
}
