//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
)

const syntheticSources = "sources:\n- path: .\n  kind: git\n  config: {repository: \"git://synthetic.invalid/app.git\", ref: main}\n"

func publishHumanGoal(t *testing.T, root, id string, files map[string]string) string {
	t.Helper()
	b, err := New(root, t.TempDir(), func(context.Context, string, []string) error { return nil }, testObservation())
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposalForPhase(context.Background(), id, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	for path, content := range files {
		target := filepath.Join(p.Path, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := b.Publish(context.Background(), id, Human, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	return receipt.Commit
}

// The commit that publishes a sources file records the accepted source commit
// it was based on as SDO-Upstream; a goal commit without sources carries none.
func TestHumanGoalCommitRecordsInitialUpstreamRevisionOnlyWithSources(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, root)
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "seed")
	seeded := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	goal := "---\nschema_version: 1\nowner: human\napplication: \"test\"\n---\n# Health objective\n\nGoal\n"

	plain := publishHumanGoal(t, root, "goal-plain", map[string]string{layout.GoalFile: goal})
	if message := testutil.Git(t, root, "log", "-1", "--format=%B", plain); strings.Contains(message, "SDO-Upstream") {
		t.Fatalf("goal without sources carries an upstream trailer:\n%s", message)
	}
	head := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	if head == seeded {
		t.Fatal("goal commit not accepted")
	}

	withSources := publishHumanGoal(t, root, "goal-sources", map[string]string{layout.GoalFile: goal + "\nUpdated\n", layout.SourcesFile: syntheticSources})
	message := "\n" + testutil.Git(t, root, "log", "-1", "--format=%B", withSources) + "\n"
	if !strings.Contains(message, "\nSDO-Upstream: "+head+"\n") {
		t.Fatalf("goal commit with sources lacks SDO-Upstream: %s:\n%s", head, message)
	}
}
