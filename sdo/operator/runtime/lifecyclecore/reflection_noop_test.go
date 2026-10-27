package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionEmptyDiffRequiresBrokerAcceptance(t *testing.T) {
	for _, decision := range []*contracts.ReflectionDecision{
		{LearningDecision: "no_change", NoChangeReason: "already covered"},
		{LearningDecision: "no_change", NoChangeReason: "missing observation", DeclineReason: "no_recorded_repair"},
		{LearningDecision: "sdk_gap", NoChangeReason: "unsupported observation"},
		{LearningDecision: "updated"},
	} {
		t.Run(decision.GetLearningDecision()+"/"+decision.GetDeclineReason(), func(t *testing.T) {
			state := &contracts.BrokerLedger{
				IncidentId: "incident", OutcomeCommit: "accepted-outcome",
				ReflectionPhase:    contracts.ReflectionPhase_REFLECTION_PHASE_REVIEW,
				ReflectionDecision: decision,
			}
			_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
			if len(effects) != 1 || effects[0].Kind != ReviewReflection {
				t.Fatalf("missing review checkpoint: %v", effects)
			}
			next, _ := StepReflection(state, ReflectionEvent{Kind: ReflectionReviewed, EffectID: effects[0].ID, At: time.Unix(100, 0)})
			_, effects = StepReflection(next, ReflectionEvent{Kind: ReflectionDrive})
			if len(effects) != 1 || effects[0].Kind != PublishReflection || !proto.Equal(effects[0].Decision, decision) {
				t.Fatalf("empty diff bypassed authoritative broker acceptance: state=%v effects=%v", next, effects)
			}
			accepted, _ := StepReflection(next, ReflectionEvent{Kind: ReflectionPublished, EffectID: effects[0].ID, At: time.Unix(101, 0)})
			if accepted.GetReflectionPhase() != contracts.ReflectionPhase_REFLECTION_PHASE_DONE || accepted.GetReflectionCommit() != "" {
				t.Fatalf("broker-approved no-op must finish without an empty commit: %v", accepted)
			}
		})
	}
}
