package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk/sdktest"
)

func TestEvidenceGapsInspectEveryCallAndBothProtectedOpenBoundaries(t *testing.T) {
	resource := repeatService
	args, _ := json.Marshal([]any{objectRefState(resource)})
	for _, name := range []string{"open@detect", "open@pre-apply"} {
		t.Run(name, func(t *testing.T) {
			capture := t.TempDir()
			calls := make([]sdktest.PrimitiveCall, 27)
			calls = append(calls, sdktest.PrimitiveCall{Method: "DiffAgainstSource", Arguments: args, Error: "recorded failure"}, sdktest.PrimitiveCall{Phase: "fixed", Method: "DiffAgainstSource", Arguments: args})
			raw, _ := json.Marshal(calls)
			if err := os.WriteFile(filepath.Join(capture, name+"-recordings.json"), raw, 0644); err != nil {
				t.Fatal(err)
			}
			work := []*contracts.ReflectionCauseWork{{Resources: []*contracts.ObjectRef{resource}}}
			if count := appendEvidenceGaps(work, capture); count != 0 || len(work[0].Items) != 0 {
				t.Fatalf("late observation became a gap: %v", work)
			}
		})
	}
}

func TestEvidenceGapsNameOnlyTheMissingResourceAndPhase(t *testing.T) {
	capture := t.TempDir()
	args, _ := json.Marshal([]any{objectRefState(repeatService)})
	raw, _ := json.Marshal([]sdktest.PrimitiveCall{{Method: "Logs", Arguments: args}})
	if err := os.WriteFile(filepath.Join(capture, "open@detect-recordings.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	work := []*contracts.ReflectionCauseWork{{CauseIndex: 2, Resources: []*contracts.ObjectRef{repeatService}}}
	if count := appendEvidenceGaps(work, capture); count != 1 {
		t.Fatalf("gap count=%d", count)
	}
	item := work[0].Items[0]
	if item.Reason != "evidence_gap" || item.EvidencePhase != "fixed" || item.Resources[0].GetName() != repeatService.GetName() || work[0].CauseIndex != 2 {
		t.Fatalf("gap=%v", work)
	}
}

func TestCaptureGapNeverExcusesLearning(t *testing.T) {
	// Capture availability is not an exception to the complete shared validator's
	// decision rules. The lifecycle review no longer has a separate content gate.
	for _, count := range []uint32{0, 1} {
		ds := validate.ReflectionDiagnostics(&contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "evidence gap", DeclineReason: "evidence_gap"}, nil)
		if len(ds) != 1 || ds[0].Rule != "reflection.decision" {
			t.Fatalf("capture gap %d excused learning: %v", count, ds)
		}
	}
}

func TestEvidenceGapCountProjectsSavedSettlementWithoutRescanning(t *testing.T) {
	count := uint32(2)
	outcome := &contracts.OutcomeRecord{ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, EvidenceGapCount: &count}}
	original := proto.Clone(outcome).(*contracts.OutcomeRecord)
	ledger := &contracts.BrokerLedger{IncidentId: "saved", OutcomeDocument: outcome}
	next, _ := lifecyclecore.StepReflection(ledger, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionBegin, MaxRetries: 2})
	if next.ReflectionEvidenceGapCount == nil || next.GetReflectionEvidenceGapCount() != 2 || !proto.Equal(next.OutcomeDocument, original) {
		t.Fatalf("settled count lost: %v", next)
	}
	// The saved projection remains stable through retry/restart dispatch.
	again, _ := lifecyclecore.StepReflection(next, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionBegin, MaxRetries: 2})
	if again.GetReflectionEvidenceGapCount() != 2 {
		t.Fatal("restart changed the settled count")
	}
	unknown, _ := lifecyclecore.StepReflection(&contracts.BrokerLedger{}, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionBegin})
	if unknown.ReflectionEvidenceGapCount != nil {
		t.Fatal("historical absence became zero")
	}
}
