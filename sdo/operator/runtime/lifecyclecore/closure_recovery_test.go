package lifecyclecore

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"time"
)

func TestFreezeCopiesRenewedRecoveryProof(t *testing.T) {
	request := &contracts.IncidentRequest{IncidentId: "incident", RepositoryBaseCommit: "base"}
	state := &contracts.ControllerState{ClosureState: contracts.ClosureState_CLOSURE_STATE_PENDING, PendingClosure: &contracts.IncidentClosure{Request: request}}
	before := proto.Clone(state)
	proof := &contracts.IncidentClosure{Request: proto.Clone(request).(*contracts.IncidentRequest), VerifiedAt: timestamppb.New(time.Unix(32, 0)), HealthClearedAt: timestamppb.New(time.Unix(17, 0)), FinalDetectorStates: []*contracts.DetectorEvaluation{{DetectorId: "traffic-health"}}}
	next, effects := Step(state, Event{Kind: Freeze, EffectID: Identity("incident", "base", FreezeClosure), Closure: proof})
	if !proto.Equal(state, before) || len(effects) != 0 || !next.PendingClosure.Frozen {
		t.Fatal("freeze mutated input or did not freeze")
	}
	proof.VerifiedAt.Seconds = 99
	proof.HealthClearedAt.Seconds = 99
	proof.FinalDetectorStates[0].DetectorId = "changed"
	if next.PendingClosure.VerifiedAt.Seconds != 32 || next.PendingClosure.HealthClearedAt.Seconds != 17 || next.PendingClosure.FinalDetectorStates[0].DetectorId != "traffic-health" {
		t.Fatal("frozen proof aliases event input")
	}
}

func TestOnlyUnpublishedPendingClosureReturnsToObservation(t *testing.T) {
	for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING, contracts.ClosureState_CLOSURE_STATE_FAILED} {
		for _, frozen := range []bool{false, true} {
			state := &contracts.ControllerState{ClosureState: phase, PendingClosure: &contracts.IncidentClosure{Request: &contracts.IncidentRequest{IncidentId: "incident"}, Frozen: frozen}}
			before := proto.Clone(state)
			next, effects := Step(state, Event{Kind: Reobserve})
			if !proto.Equal(state, before) || len(effects) != 0 {
				t.Fatal("reobserve mutated input or emitted effects")
			}
			allowed := phase == contracts.ClosureState_CLOSURE_STATE_PENDING && !frozen
			if allowed {
				if next.PendingClosure != nil || next.ClosureState != contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED {
					t.Fatal("unpublished closure did not return ownership")
				}
			} else if !proto.Equal(next, state) {
				t.Fatal("reobserve rewrote a committed/frozen command")
			}
		}
	}
}
