//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
)

// memory.conflict is the broker's rule, not validate.Check's: it appears when
// a proposal's base is no longer the accepted head and the rebased change cannot
// merge. It has no gate-table row because the table runs Check on one tree; this
// test drives the broker over a real repository with two proposals.
func TestConcurrentProposalsOnOnePlaybookConflictWithoutChangingAcceptedState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	files := map[string]string{
		"app.txt":                       "base\n",
		".sdo/schema-version":           "3\n",
		".sdo/goal.md":                  "---\nschema_version: 1\nowner: human\napplication: test\n---\nGoal\n",
		".sdo/arch.md":                  "---\nschema_version: 1\ngenerated_at_commit: base\ngenerated_at: 2026-01-01T00:00:00Z\napplication: test\ntopology_fingerprint: abc\n---\nArchitecture\n",
		".sdo/playbooks/README.md":      "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md": playbook("Original"),
	}
	for path, content := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "accepted memory")
	accept := func(context.Context, string, []string) error { return nil }
	lanes := map[string]*Broker{}
	for _, id := range []string{"lane-a", "lane-b"} {
		b, err := New(root, t.TempDir(), accept)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.RecordOutcome(context.Background(), id, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}); err != nil {
			t.Fatal(err)
		}
		lanes[id] = b
	}
	proposals := map[string]Proposal{}
	for id, b := range lanes {
		p, err := b.CreateProposalForPhase(context.Background(), id, ReflectionPhase)
		if err != nil {
			t.Fatal(err)
		}
		proposals[id] = p
		if err := os.WriteFile(filepath.Join(p.Path, ".sdo/playbooks/base/README.md"), []byte(playbook(id+" intent")), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	publish := func(id string) (Receipt, error) {
		return lanes[id].PublishReflection(context.Background(), id, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}})
	}
	first, err := publish("lane-a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = publish("lane-b")
	diagnostics, typed := RejectionDiagnostics(err)
	if !typed || len(diagnostics) == 0 || diagnostics[0].Rule != MemoryConflictRule {
		t.Fatalf("want a typed %s, got %v", MemoryConflictRule, err)
	}
	conflict := diagnostics[0].GetMemoryConflict()
	if conflict == nil || conflict.GetBaseCommit() != proposals["lane-b"].BaseCommit || conflict.GetAcceptedCommit() != first.Commit ||
		!slices.Contains(conflict.GetConflictingPaths(), ".sdo/playbooks/base/README.md") ||
		len(conflict.GetAcceptedChanges()) != 1 || conflict.GetAcceptedChanges()[0].GetIncidentId() != "lane-a" {
		t.Fatalf("conflict lacks the accepted provenance: %v", conflict)
	}
	if !strings.Contains(diagnostics[0].Message, first.Commit) {
		t.Fatalf("message omits the accepted commit: %s", diagnostics[0].Message)
	}
	if head := testutil.Git(t, root, "rev-parse", "HEAD"); head != first.Commit {
		t.Fatalf("a rejected proposal moved the accepted head to %s", head)
	}
}

func playbook(text string) string {
	return "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: synthetic\n---\n" + text + " <RESOURCE_NAME>\n"
}
