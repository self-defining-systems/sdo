package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The accepted commit's coverage gaps are durable ledger facts, so analysis
// counts them without parsing commit trailers.
func TestPublishedReflectionRecordsCoverageGaps(t *testing.T) {
	state := &contracts.BrokerLedger{
		IncidentId: "incident", OutcomeCommit: "accepted-outcome",
		ReflectionPhase:    contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH,
		ReflectionDecision: &contracts.ReflectionDecision{LearningDecision: "updated"},
	}
	_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
	if len(effects) != 1 || effects[0].Kind != PublishReflection {
		t.Fatalf("want one publish effect, got %v", effects)
	}
	gaps := []*contracts.MemoryDiagnostic{
		{Rule: "reflection.program-required", Message: "verified cause 1 has no replayable program"},
		{Rule: "program.repeat-unreachable", Message: "no finding selects this playbook"},
	}
	accepted, _ := StepReflection(state, ReflectionEvent{Kind: ReflectionPublished, EffectID: effects[0].ID, At: time.Unix(101, 0), Commit: "abc", CoverageGaps: gaps})
	if accepted.GetReflectionPhase() != contracts.ReflectionPhase_REFLECTION_PHASE_DONE || len(accepted.GetReflectionCoverageGaps()) != 2 {
		t.Fatalf("coverage gaps not recorded: %v", accepted)
	}
	for i, gap := range gaps {
		if !proto.Equal(gap, accepted.GetReflectionCoverageGaps()[i]) {
			t.Fatalf("gap %d changed: %v", i, accepted.GetReflectionCoverageGaps()[i])
		}
	}
	gaps[0].Rule = "mutated"
	if accepted.GetReflectionCoverageGaps()[0].Rule == "mutated" {
		t.Fatal("ledger aliases the event's gaps")
	}
}
