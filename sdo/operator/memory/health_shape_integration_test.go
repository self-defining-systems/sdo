//go:build integration

package memory_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const goalFile = "---\nkind: goal\n---\n# Health objective\nkeep it up\n"

// judgedRepo has a health_judge commit followed by one source commit.
func judgedRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q")
	writeFile(t, dir, ".sdo/goal.md", goalFile)
	writeFile(t, dir, "main.go", "package main // v1\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed")
	writeFile(t, dir, "main.go", "package main // v2\n")
	gitIn(t, dir, "commit", "-qam", "benign source change")
	return dir
}

func TestAcceptedHealthRevisionNeedsRefreshOnShapeNotBytes(t *testing.T) {
	dir := judgedRepo(t)
	ctx := context.Background()
	if rev, err := memory.AcceptedHealthRevision(ctx, dir, nil); err != nil || !rev.NeedsRefresh {
		t.Fatalf("bytes fallback (nil ShapeOf): NeedsRefresh = %v, err = %v; want true", rev.NeedsRefresh, err)
	}
	constant := func(context.Context, string, string) (string, error) { return "shape-1", nil }
	rev, err := memory.AcceptedHealthRevision(ctx, dir, constant)
	if err != nil {
		t.Fatal(err)
	}
	if rev.NeedsRefresh {
		t.Fatal("source bytes changed but shape did not: no refresh expected")
	}
	if rev.SourceInputs == "" {
		t.Fatal("bytes must still be reported for adoption")
	}
	byRevision := func(_ context.Context, _ string, revision string) (string, error) {
		if revision == rev.JudgeCommit {
			return "shape-1", nil
		}
		return "shape-2", nil
	}
	if rev, err = memory.AcceptedHealthRevision(ctx, dir, byRevision); err != nil || !rev.NeedsRefresh {
		t.Fatalf("shape changed: NeedsRefresh = %v, err = %v; want true", rev.NeedsRefresh, err)
	}
}

func TestAcceptedHealthRevisionNeedsRefreshOnObjective(t *testing.T) {
	dir := judgedRepo(t)
	writeFile(t, dir, ".sdo/goal.md", strings.Replace(goalFile, "keep it up", "new objective", 1))
	gitIn(t, dir, "commit", "-qam", "objective")
	constant := func(context.Context, string, string) (string, error) { return "shape-1", nil }
	rev, err := memory.AcceptedHealthRevision(context.Background(), dir, constant)
	if err != nil || !rev.NeedsRefresh {
		t.Fatalf("objective changed: NeedsRefresh = %v, err = %v; want true", rev.NeedsRefresh, err)
	}
}

func TestAcceptedHealthRevisionShapeErrorIsNotSwallowed(t *testing.T) {
	dir := judgedRepo(t)
	failing := func(context.Context, string, string) (string, error) { return "", os.ErrInvalid }
	if _, err := memory.AcceptedHealthRevision(context.Background(), dir, failing); err == nil {
		t.Fatal("ShapeOf failure must surface")
	}
}

// A seed has no health_judge commit, so its provenance is unknown and the install
// must run the (scripted) judge refresh: this is by design, not a regression of
// shape-keyed revisions, which only compare when a judge commit exists.
func TestAcceptedHealthRevisionOfASeedWithoutJudgeCommitNeedsRefresh(t *testing.T) {
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q")
	writeFile(t, dir, "main.go", "package main\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "feat: seed application")
	writeFile(t, dir, ".sdo/goal.md", goalFile)
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "feat: seed operational memory")
	same := func(context.Context, string, string) (string, error) { return "shape-1", nil }
	for name, shapeOf := range map[string]memory.ShapeOf{"bytes": nil, "shape": same} {
		rev, err := memory.AcceptedHealthRevision(context.Background(), dir, shapeOf)
		if err != nil || !rev.NeedsRefresh || rev.JudgeCommit != "" {
			t.Fatalf("%s: NeedsRefresh = %v, JudgeCommit = %q, err = %v; want a refresh with no judge commit", name, rev.NeedsRefresh, rev.JudgeCommit, err)
		}
	}
	gitIn(t, dir, "commit", "-q", "--allow-empty", "-m", "judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed")
	if rev, err := memory.AcceptedHealthRevision(context.Background(), dir, same); err != nil || rev.NeedsRefresh {
		t.Fatalf("after the judge commit with nothing changed: NeedsRefresh = %v, err = %v; want false", rev.NeedsRefresh, err)
	}
}
