package memory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
)

func appRepoWithout(t *testing.T, drop ...string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.InitGit(t, dir)
	files := map[string]string{
		"app.txt":               "app\n",
		layout.ArchitectureFile: "---\nschema_version: 1\ngenerated_at_commit: abc\napplication: \"toy\"\n---\ninventory\n",
	}
	if err := WriteGoal(dir, "toy", "serve", nil); err != nil {
		t.Fatal(err)
	}
	for rel, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range drop {
		_ = os.Remove(filepath.Join(dir, filepath.FromSlash(rel)))
	}
	testutil.Git(t, dir, "add", "-A", "-f")
	testutil.Git(t, dir, "commit", "-qm", "seed")
	return dir
}

// Install supplies SDO's standard goal when the accepted memory has none, in a
// human commit, once; the start-up reads then succeed.
func TestEnsureGoalSuppliesTheStandardGoalOnce(t *testing.T) {
	ctx := context.Background()
	repo := appRepoWithout(t, layout.GoalFile)
	if _, err := AcceptedHealthRevision(ctx, repo, nil); err == nil {
		t.Fatal("precondition: a goal-less repository must fail the health read")
	}
	added, err := EnsureGoal(ctx, repo, "toy")
	if err != nil || !added {
		t.Fatalf("EnsureGoal = %v, %v", added, err)
	}
	if msg := testutil.Git(t, repo, "log", "-1", "--format=%B"); !strings.Contains(msg, "SDO-Actor: human") {
		t.Fatalf("goal commit %q", msg)
	}
	if _, err := AcceptedHealthRevision(ctx, repo, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := AcceptedHealthObjective(repo); err != nil || got != DefaultHealthObjective {
		t.Fatalf("objective = %q, %v", got, err)
	}
	head := testutil.Git(t, repo, "rev-parse", "HEAD")
	if added, err := EnsureGoal(ctx, repo, "toy"); err != nil || added || testutil.Git(t, repo, "rev-parse", "HEAD") != head {
		t.Fatalf("second EnsureGoal = %v, %v", added, err)
	}
}

// An existing human goal is never replaced.
func TestEnsureGoalKeepsTheHumanGoal(t *testing.T) {
	repo := appRepoWithout(t)
	head := testutil.Git(t, repo, "rev-parse", "HEAD")
	if added, err := EnsureGoal(context.Background(), repo, "toy"); err != nil || added || testutil.Git(t, repo, "rev-parse", "HEAD") != head {
		t.Fatalf("EnsureGoal = %v, %v", added, err)
	}
}

// Parity: every artifact the start-up reads is either supplied by install
// (EnsureGoal) or required by the shared memory check. Removing each in turn
// from an otherwise minimal memory must leave the start-up reads working once
// install has run; the architecture is required by the check
// (memory.architecture-missing), so the start-up reads here do not need it.
func TestStartUpReadsSurviveEachArtifactRemoved(t *testing.T) {
	ctx := context.Background()
	for _, drop := range []string{layout.GoalFile, layout.SourcesFile, layout.ArchitectureFile, layout.DeploymentPlanFile, layout.DiagnosticsGoModFile, layout.DetectorsDir + "/health/x.go"} {
		t.Run(drop, func(t *testing.T) {
			repo := appRepoWithout(t, drop)
			if _, err := EnsureGoal(ctx, repo, "toy"); err != nil {
				t.Fatal(err)
			}
			if _, err := AcceptedHealthRevision(ctx, repo, nil); err != nil {
				t.Fatalf("health revision: %v", err)
			}
			if _, err := AcceptedHealthObjective(repo); err != nil {
				t.Fatalf("objective: %v", err)
			}
		})
	}
}
