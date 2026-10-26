package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestResponseExpiryArchivesOnlyUncommittedClosure(t *testing.T) {
	reasons := map[EventKind]contracts.ClosureFailureReason{
		ResponseExpired: contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT,
		OwnerLost:       contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_OWNER_LOST,
	}
	for kind, reason := range reasons {
		for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING} {
			t.Run(reason.String()+"/"+phase.String(), func(t *testing.T) {
				state := &contracts.ControllerState{PendingClosure: modelClosure(1), ClosureState: phase}
				before := proto.Clone(state)
				next, effects := Step(state, Event{Kind: kind, At: time.Unix(301, 0), Error: "incident work has no live owner"})
				if !proto.Equal(state, before) || len(effects) != 0 {
					t.Fatal("expiry mutated input or launched work")
				}
				if phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
					if !proto.Equal(next, state) {
						t.Fatal("expiry discarded already committed outcome")
					}
					return
				}
				if BlocksNext(next.ClosureState, true) || len(next.FailedClosures) != 1 || next.FailedClosures[0].Failure.Reason != reason || !next.FailedClosures[0].Failure.Permanent {
					t.Fatalf("expiry did not archive terminal typed reason: %v", next)
				}
				restarted, _ := Step(next, Event{Kind: Restart})
				if !proto.Equal(next, restarted) {
					t.Fatal("restart reopened expired closure")
				}
			})
		}
	}
}
