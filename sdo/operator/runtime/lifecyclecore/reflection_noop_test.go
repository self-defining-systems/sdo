package lifecyclecore

import (
	"fmt"
	"strings"
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

// Every exhausted correction ends with an explicit failed decision. The
// original worker results and validator diagnostics remain immutable evidence.
func TestReflectionExhaustionRetainsTerminalDecision(t *testing.T) {
	for _, decision := range []string{"updated", "no_change", "sdk_gap"} {
		for _, retries := range []uint32{0, 2, 5} {
			t.Run(fmt.Sprintf("%s/retries=%d", decision, retries), func(t *testing.T) {
				original := &contracts.ReflectionDecision{LearningDecision: decision, NoChangeReason: "synthetic rejected decision"}
				state := &contracts.BrokerLedger{IncidentId: "incident", OutcomeCommit: "accepted-outcome",
					ReflectionPhase:    contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH,
					ReflectionDecision: original, ReflectionResults: []*contracts.ReflectionDecision{original},
					ReflectionMaxRetries: retries}
				for attempt := uint32(0); attempt <= retries; attempt++ {
					_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
					state, _ = StepReflection(state, ReflectionEvent{Kind: ReflectionRejected, EffectID: effects[0].ID,
						At: time.Unix(int64(attempt+1), 0), Diagnostics: []*contracts.MemoryDiagnostic{{Rule: "synthetic.rejection", Message: "synthetic invalid memory"}}})
					if attempt < retries {
						// The next publication still carries the real validator's
						// previous rejections, just as a corrected worker result does.
						state.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH
					}
				}
				_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
				if len(effects) != 1 || effects[0].Kind != ResetReflection || !effects[0].Final {
					t.Fatalf("missing final checkout cleanup: %v", effects)
				}
				state, _ = StepReflection(state, ReflectionEvent{Kind: ReflectionReset, EffectID: effects[0].ID})
				if state.GetReflectionPhase() != contracts.ReflectionPhase_REFLECTION_PHASE_DONE || state.GetReflectionDecision().GetLearningDecision() != "failed" || !strings.Contains(state.GetReflectionDecision().GetError(), "synthetic invalid memory") {
					t.Fatalf("exhausted reflection lost its terminal reason: %v", state)
				}
				if len(state.ReflectionRejections) != int(retries+1) || len(state.ReflectionResults) != 1 || !proto.Equal(state.ReflectionResults[0], original) {
					t.Fatalf("terminal failure changed worker/validator evidence: %v", state)
				}
			})
		}
	}
}
