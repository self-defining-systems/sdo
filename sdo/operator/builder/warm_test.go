package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

// Warming the Go build cache for an accepted memory is the controller's own start-up build with the artifact thrown
// away: the same staged workspace, `go mod tidy` and the same `go build` of the memory host, so every package the
// controller will need is compiled under the cache keys it will look up. It runs no test and links nothing else.
func TestWarmAcceptedMemoryHostRunsTheControllersStartupBuildAndKeepsNothing(t *testing.T) {
	app := t.TempDir()
	if err := os.CopyFS(filepath.Join(app, ".sdo"), os.DirFS(playbookProgramFixture)); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, app)
	testutil.Git(t, app, "add", ".")
	testutil.Git(t, app, "commit", "-qm", "synthetic memory")
	head := testutil.Git(t, app, "rev-parse", "HEAD")
	stages := testutil.FakeGo(t)

	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	commit, err := WarmAcceptedMemoryHost(context.Background(), Config{ApplicationRoot: app, OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	if commit != head {
		t.Fatalf("warmed %s, the accepted commit is %s", commit, head)
	}
	log, err := os.ReadFile(stages)
	if err != nil {
		t.Fatal(err)
	}
	var verbs []string
	for _, line := range strings.Split(strings.TrimSpace(string(log)), "\n") {
		_, args, _ := strings.Cut(line, "\t")
		verbs = append(verbs, args)
	}
	if len(verbs) != 2 || verbs[0] != "mod tidy" ||
		!strings.HasPrefix(verbs[1], "build -trimpath ") || !strings.Contains(verbs[1], " -buildvcs=false -o ") || !strings.HasSuffix(verbs[1], "./cmd/memory-host") {
		t.Fatalf("the warm build is not the controller's start-up build (mod tidy, then build of the memory host): %q", verbs)
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Fatalf("the warm build left its workspace behind: %v %v", left, err)
	}
}

func TestWarmAcceptedMemoryHostReportsABuildThatFails(t *testing.T) {
	app := t.TempDir()
	if err := os.CopyFS(filepath.Join(app, ".sdo"), os.DirFS(playbookProgramFixture)); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, app)
	testutil.Git(t, app, "add", ".")
	testutil.Git(t, app, "commit", "-qm", "synthetic memory")
	testutil.FakeGo(t, testutil.GoRule{Verb: "build", Output: "synthetic compile error"})
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	if _, err := WarmAcceptedMemoryHost(context.Background(), Config{ApplicationRoot: app, OperatorRoot: ".."}); err == nil || !strings.Contains(err.Error(), "synthetic compile error") {
		t.Fatalf("a failed warm build must name the compiler's error: %v", err)
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Fatalf("a failed warm build left its workspace behind: %v", left)
	}
}
