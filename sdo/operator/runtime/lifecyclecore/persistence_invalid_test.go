package lifecyclecore

import (
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestPersistenceInvalidNeverReplacesAcceptedReceipt(t *testing.T) {
	closure := &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "incident"}, Result: &contracts.IncidentResult{Status: "failed"}, StatePersistenceFailure: &contracts.StatePersistenceFailure{Reason: "state_persistence_invalid", Detail: "result.failure_reason"}}
	for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING} {
		t.Run(phase.String(), func(t *testing.T) {
			state := &contracts.ControllerState{ClosureState: phase, ClosureReceipt: &contracts.ClosureReceipt{IncidentId: "incident", OutcomeCommit: "accepted"}}
			before := proto.Clone(state)
			next, effects := Step(state, Event{Kind: PersistenceInvalid, Closure: closure})
			if !proto.Equal(state, before) || len(effects) != 0 {
				t.Fatal("recovery mutated input or ran before persistence")
			}
			if phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
				if !proto.Equal(next, state) {
					t.Fatal("recovery replaced accepted receipt")
				}
			} else if next.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_PENDING || !proto.Equal(next.GetPendingClosure(), closure) || next.GetClosureReceipt() != nil {
				t.Fatalf("recovery did not queue failed closure: %v", next)
			}
		})
	}
}
