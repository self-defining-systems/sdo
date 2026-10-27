package runtime

import (
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

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
