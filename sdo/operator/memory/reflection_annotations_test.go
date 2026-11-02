package memory

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionRejectionsAreValidatedAppendOnlyAnnotations(t *testing.T) {
	ctx := context.Background()
	s := NewJSONStore(t.TempDir())
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success", SourceCommit: "source"}
	if _, err := s.Put(ctx, original); err != nil {
		t.Fatal(err)
	}
	first := &contracts.ProposalRejection{Attempt: 1, Diagnostics: []*contracts.MemoryDiagnostic{{Rule: "memory.conflict", Message: "accepted peer intent conflicts"}}}
	if _, err := s.AppendReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{first}); err != nil {
		t.Fatal(err)
	}
	second := proto.Clone(first).(*contracts.ProposalRejection)
	second.Attempt = 2
	for _, entry := range []*contracts.ProposalRejection{first, second} {
		if _, err := s.AppendReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{entry}); err != nil {
			t.Fatal(err)
		}
	}
	again := proto.Clone(first).(*contracts.ProposalRejection)
	again.RejectedAt = timestamppb.Now()
	if _, err := s.AppendReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{again}); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []*contracts.ProposalRejection{nil, {Attempt: 0, Diagnostics: first.Diagnostics}, {Attempt: 3}} {
		if _, err := s.AppendReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{invalid}); err == nil {
			t.Fatalf("invalid rejection accepted: %v", invalid)
		}
	}
	retry, err := s.Put(ctx, original)
	if err != nil || len(retry.Record.GetReflectionRejections()) != 2 {
		t.Fatalf("immutable outcome retry lost annotations: %+v %v", retry.Record, err)
	}
	forged := proto.Clone(retry.Record).(*contracts.OutcomeRecord)
	forged.ReflectionRejections[0].Diagnostics[0].Message = "replace accepted rejection"
	if _, err := s.Put(ctx, forged); err == nil {
		t.Fatal("outcome retry replaced accepted rejection")
	}
	got, err := s.Get(ctx, "incident")
	if err != nil || got.Record.SourceCommit != "source" || got.Record.Classification != "success" || len(got.Record.ReflectionRejections) != 2 || !proto.Equal(first, got.Record.ReflectionRejections[0]) {
		t.Fatalf("rejection annotation mutated incident evidence: %+v %v", got.Record, err)
	}
}
