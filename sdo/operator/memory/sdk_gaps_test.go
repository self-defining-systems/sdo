package memory

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestAppendSDKGapsPreservesIncidentEvidence(t *testing.T) {
	ctx := context.Background()
	s := NewJSONStore(t.TempDir())
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "i", Classification: "success", SourceCommit: "source"}
	if _, err := s.Put(ctx, original); err != nil {
		t.Fatal(err)
	}
	gap := &contracts.SdkGap{FaultClass: "class", Primitive: "operation", Reason: "not expressible", ScratchScript: "fixture-command"}
	if _, err := s.AppendSDKGaps(ctx, "i", []*contracts.SdkGap{gap}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendSDKGaps(ctx, "i", []*contracts.SdkGap{gap}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "i")
	if err != nil || got.Record.SourceCommit != "source" || got.Record.Classification != "success" || len(got.Record.SdkGaps) != 1 {
		t.Fatalf("annotation changed evidence: %+v %v", got, err)
	}
	if _, err = s.Put(ctx, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "i", Classification: "failed"}); err == nil {
		t.Fatal("base outcome became mutable")
	}
}
func TestOriginalOutcomeRetryPreservesSDKGapAnnotation(t *testing.T) {
	ctx := context.Background()
	s := NewJSONStore(t.TempDir())
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "i", Classification: "success"}
	if _, err := s.Put(ctx, original); err != nil {
		t.Fatal(err)
	}
	gap := &contracts.SdkGap{FaultClass: "x", Primitive: "op", Reason: "gap", ScratchScript: "fixture"}
	if _, err := s.AppendSDKGaps(ctx, "i", []*contracts.SdkGap{gap}); err != nil {
		t.Fatal(err)
	}
	outcome, err := s.Put(ctx, original)
	if err != nil || len(outcome.Record.GetSdkGaps()) != 1 {
		t.Fatalf("original delivery retry lost annotation: %v", err)
	}
}
