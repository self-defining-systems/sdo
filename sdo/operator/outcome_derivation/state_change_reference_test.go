package outcome_derivation

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestParseKeyRejectsCompoundReferences(t *testing.T) {
	for _, source := range []string{
		"Service/geo; Service/recommendation",
		"Service/geo, Service/recommendation",
		"Service/geo Service/recommendation",
		"Service/geo;/Service/recommendation",
	} {
		if key, ok := parseKey(source); ok {
			t.Errorf("parseKey(%q) = %+v, want rejection", source, key)
		}
	}
	for source, want := range map[string]objectKey{
		"Service/geo":       {kind: "service", name: "geo", label: "Service/geo"},
		"Service/hotel/geo": {kind: "service", name: "geo", namespace: "hotel", label: "Service/geo"},
	} {
		if key, ok := parseKey(source); !ok || key != want {
			t.Errorf("parseKey(%q) = %+v, %v, want %+v", source, key, ok, want)
		}
	}
}

func TestDeriveVerifiesEachSeparateStateChangeEntry(t *testing.T) {
	closure := fixtureClosure()
	stamp := func(offset time.Duration) *timestamppb.Timestamp {
		return timestamppb.New(closure.DetectedAt.AsTime().Add(offset))
	}
	cause := closure.Result.ConfirmedRootCauses[0]
	cause.Evidence = []*contractsv1alpha1.RootCauseEvidence{
		{Kind: "state-change", Source: "Service/geo", Observation: "targetPort changed"},
		{Kind: "state-change", Source: "Service/recommendation", Observation: "targetPort changed"},
	}
	closure.ObservedStateChanges = []*contractsv1alpha1.ObservedStateChange{
		{Kind: "Service", Name: "geo", FirstObservedAt: stamp(10 * time.Second)},
		{Kind: "Service", Name: "recommendation", FirstObservedAt: stamp(10 * time.Second)},
	}
	result, err := Derive(context.Background(), closure, "repair-commit", "codex", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	verification := result.Record.GetDiagnosisVerification()[0]
	for _, check := range verification.GetEvidence()[:2] {
		if check.Verified == nil || !*check.Verified {
			t.Fatalf("separate state-change entry not verified: %+v", check)
		}
	}
	if verification.GetVerdict() != "confirmed" {
		t.Fatalf("verdict = %s, want confirmed: %+v", verification.GetVerdict(), verification)
	}
}
