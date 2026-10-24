package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestResponseExpiryArchivesOnlyUncommittedClosure(t *testing.T) {
	for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING} {
		t.Run(phase.String(), func(t *testing.T) {
			state := &contracts.ControllerState{PendingClosure: modelClosure(1), ClosureState: phase}
			before := proto.Clone(state)
			next, effects := Step(state, Event{Kind: ResponseExpired, At: time.Unix(301, 0), Error: "response deadline expired with no live work"})
			if !proto.Equal(state, before) || len(effects) != 0 {
				t.Fatal("expiry mutated input or launched work")
			}
			if phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
				if !proto.Equal(next, state) {
					t.Fatal("expiry discarded already committed outcome")
				}
				return
			}
			if BlocksNext(next.ClosureState, true) || len(next.FailedClosures) != 1 || next.FailedClosures[0].Failure.Reason != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_AGENT_TIMEOUT {
				t.Fatalf("expiry did not archive terminal typed reason: %v", next)
			}
			restarted, _ := Step(next, Event{Kind: Restart})
			if !proto.Equal(next, restarted) {
				t.Fatal("restart reopened expired closure")
			}
		})
	}
}
