package lifecyclecore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionMemoryConflictStopsAfterThreeAttemptsAcrossRestart(t *testing.T) {
	state := &contracts.BrokerLedger{IncidentId: "loser", OutcomeCommit: "outcome", ReflectionMaxRetries: 5}
	for attempt := 1; attempt <= 3; attempt++ {
		state.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH
		_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
		diagnostic := &contracts.MemoryDiagnostic{Rule: "memory.conflict", Message: "preserve peer intent", MemoryConflict: &contracts.MemoryConflict{BaseCommit: "old", AcceptedCommit: "new", ConflictingPaths: []string{"book"}}}
		state, _ = StepReflection(state, ReflectionEvent{Kind: ReflectionRejected, EffectID: effects[0].ID, At: time.Unix(int64(attempt), 0), Diagnostics: []*contracts.MemoryDiagnostic{diagnostic}})
		if state.GetReflectionRejected() != (attempt == 3) {
			t.Fatalf("attempt %d terminal=%v", attempt, state.GetReflectionRejected())
		}
		// Restart serializes the complete conflict, including the final rejection.
		raw, err := proto.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		state = &contracts.BrokerLedger{}
		if err := proto.Unmarshal(raw, state); err != nil {
			t.Fatal(err)
		}
		_, effects = StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
		state, _ = StepReflection(state, ReflectionEvent{Kind: ReflectionReset, EffectID: effects[0].ID})
	}
	if state.GetReflectionPhase() != contracts.ReflectionPhase_REFLECTION_PHASE_DONE || len(state.GetReflectionRejections()) != 3 {
		t.Fatal("conflict retry bound not durable")
	}
	if got := state.GetReflectionRejections()[2].GetDiagnostics()[0].GetMemoryConflict(); got.GetAcceptedCommit() != "new" {
		t.Fatal("terminal diagnostic lost")
	}
}

func TestReflectionMemoryConflictAlsoBoundsInvalidCorrections(t *testing.T) {
	state := &contracts.BrokerLedger{IncidentId: "loser", OutcomeCommit: "outcome", ReflectionMaxRetries: 5}
	for attempt := 1; attempt <= 3; attempt++ {
		state.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_PUBLISH
		_, effects := StepReflection(state, ReflectionEvent{Kind: ReflectionDrive})
		diagnostic := &contracts.MemoryDiagnostic{Rule: "synthetic.invalid-correction", Message: "invalid correction"}
		if attempt == 1 {
			diagnostic.Rule = "memory.conflict"
			diagnostic.MemoryConflict = &contracts.MemoryConflict{AcceptedCommit: "peer"}
		}
		state, _ = StepReflection(state, ReflectionEvent{Kind: ReflectionRejected, EffectID: effects[0].ID, Diagnostics: []*contracts.MemoryDiagnostic{diagnostic}})
		if state.GetReflectionRejected() != (attempt == 3) {
			t.Fatalf("attempt %d terminal=%v", attempt, state.GetReflectionRejected())
		}
	}
}
