package broker

import (
	"context"
	"path/filepath"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

func TestReflectionRejectionsPreserveHistoricalOutcomeIdentity(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	workspace := filepath.Join(t.TempDir(), "broker")
	b, err := New(target, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success", SourceCommit: "source"}
	outcome, err := b.RecordOutcome(ctx, "incident", original)
	if err != nil {
		t.Fatal(err)
	}
	rejection := &contracts.ProposalRejection{Attempt: 1, Diagnostics: []*contracts.MemoryDiagnostic{{Rule: "memory.conflict", Message: "accepted peer changed the same path", MemoryConflict: &contracts.MemoryConflict{AcceptedCommit: outcome.Commit, ConflictingPaths: []string{".sdo/playbooks/base/README.md"}}}}}
	annotation, err := b.RecordReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{rejection})
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Commit == outcome.Commit {
		t.Fatal("rejection was not published")
	}
	b, err = New(target, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := b.RecordReflectionRejections(ctx, "incident", []*contracts.ProposalRejection{rejection}); err != nil || again.Commit != annotation.Commit {
		t.Fatalf("duplicate rejection published twice: %+v %v", again, err)
	}
	if _, err := b.RecordOutcome(ctx, "incident", original); err != nil {
		t.Fatal(err)
	}
	if head := git(t, target, "rev-parse", "HEAD"); head != annotation.Commit {
		t.Fatal("original outcome retry changed accepted head")
	}
	if got, ok, err := b.AcceptedCommitAt(ctx, "incident", OutcomePhase, outcome.Commit); err != nil || !ok || got.Commit != outcome.Commit {
		t.Fatalf("annotation replaced frozen outcome identity: %+v %v %v", got, ok, err)
	}
	for _, wrong := range []struct {
		id    string
		phase Phase
	}{
		{"peer", OutcomePhase},
		{"incident", ReflectionPhase},
	} {
		if _, ok, err := b.AcceptedCommitAt(ctx, wrong.id, wrong.phase, outcome.Commit); err != nil || ok {
			t.Fatalf("unrelated broker provenance authenticated: %+v %v %v", wrong, ok, err)
		}
	}
	if _, _, err := b.AcceptedCommitAt(ctx, "incident", OutcomePhase, ""); err == nil {
		t.Fatal("missing historical identity accepted")
	}
	got, err := memory.OpenReader(target).Get(ctx, "incident")
	if err != nil || got.Record.SourceCommit != "source" || got.Record.Classification != "success" || len(got.Record.ReflectionRejections) != 1 {
		t.Fatalf("annotation changed incident evidence: %+v %v", got, err)
	}
}
