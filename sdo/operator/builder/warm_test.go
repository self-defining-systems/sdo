package builder

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
)

func warmFixture(t *testing.T) (app, head string) {
	t.Helper()
	app = t.TempDir()
	if err := os.CopyFS(filepath.Join(app, ".sdo"), os.DirFS(playbookProgramFixture)); err != nil {
		t.Fatal(err)
	}
	testutil.InitGit(t, app)
	testutil.Git(t, app, "add", ".")
	testutil.Git(t, app, "commit", "-qm", "synthetic memory")
	return app, testutil.Git(t, app, "rev-parse", "HEAD")
}

func stageArgs(t *testing.T, log string) []string {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	var stages []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		_, args, _ := strings.Cut(line, "\t")
		stages = append(stages, args)
	}
	return stages
}

// Warming the Go build cache for an accepted memory is a controller start's compiles with the executables thrown away:
// the same staged workspace and the same go stages (module tidy, the test compile of every package, the detector and
// generated gates, the controller binary, the memory host), so every package a start needs is compiled under the
// cache keys it will look up. It runs nothing else and keeps nothing.
func TestWarmLaunchBuildRunsTheControllersStartupStagesAndKeepsNothing(t *testing.T) {
	app, head := warmFixture(t)
	stages := testutil.FakeGo(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	commit, err := WarmLaunchBuild(context.Background(), RunConfig{Build: Config{ApplicationRoot: app, OperatorRoot: "..", Replay: "off"}})
	if err != nil {
		t.Fatal(err)
	}
	if commit != head {
		t.Fatalf("warmed %s, the accepted commit is %s", commit, head)
	}
	got := stageArgs(t, stages)
	has := func(prefix, suffix string) bool {
		for _, stage := range got {
			if strings.HasPrefix(stage, prefix) && strings.HasSuffix(stage, suffix) {
				return true
			}
		}
		return false
	}
	if got[0] != "mod tidy" || !has("test -trimpath -exec=true -run ^$", "./...") || !has("build -trimpath", "./cmd/controller") || !has("build -trimpath", "./cmd/memory-host") {
		t.Fatalf("the warm build is not a controller start's compiles: %q", got)
	}
	for _, stage := range got {
		if strings.HasPrefix(stage, "vet") {
			t.Fatalf("a start does not vet: %q", got)
		}
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Fatalf("the warm build left its workspace behind: %v %v", left, err)
	}
}

func TestWarmLaunchBuildReportsABuildThatFailsAndKeepsNothing(t *testing.T) {
	app, _ := warmFixture(t)
	testutil.FakeGo(t, testutil.GoRule{Verb: "build", Output: "synthetic compile error"})
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	if _, err := WarmLaunchBuild(context.Background(), RunConfig{Build: Config{ApplicationRoot: app, OperatorRoot: "..", Replay: "off"}}); err == nil || !strings.Contains(err.Error(), "synthetic compile error") {
		t.Fatalf("a failed warm build must name the compiler's error: %v", err)
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Fatalf("a failed warm build left its workspace behind: %v", left)
	}
}

var stageDir = regexp.MustCompile(`-o \S+/`)

// normalized drops the staging directory, which differs per run.
func normalized(stages []string) []string {
	out := make([]string, len(stages))
	for i, stage := range stages {
		out[i] = stageDir.ReplaceAllString(stage, "-o <workspace>/")
	}
	return out
}

// The warm exists to make a controller start's compiles cache hits, so it must run exactly the stages a start runs.
// Both call compileLaunch; this pins that no stage is added to either side alone: the go stages of a real start
// (runPass, up to the point it launches the controller child) and of the warm are the same list.
func TestWarmLaunchBuildRunsTheSameStagesAsAControllerStart(t *testing.T) {
	app, _ := warmFixture(t)
	build := Config{ApplicationRoot: app, OperatorRoot: "..", Replay: "off"}

	startLog := testutil.FakeGo(t)
	t.Setenv("TMPDIR", t.TempDir())
	started := RunConfig{
		Build:      build,
		Controller: &contractsv1alpha1.ControllerConfig{},
		ResolveDiagnostics: func(ctx context.Context, root string) (DiagnosticsProjection, error) {
			projected, err := memory.AcceptedDiagnostics(ctx, root)
			return DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision}, err
		},
	}
	// the fake go links an empty executable, so launching the controller child fails after every compile has run
	_, _ = runPass(context.Background(), started)
	start := normalized(stageArgs(t, startLog))

	warmLog := testutil.FakeGo(t)
	if _, err := WarmLaunchBuild(context.Background(), RunConfig{Build: build}); err != nil {
		t.Fatal(err)
	}
	warmed := normalized(stageArgs(t, warmLog))

	if len(start) < 4 || strings.Join(start, "\n") != strings.Join(warmed, "\n") {
		t.Fatalf("a controller start and the warm compile different things\nstart: %q\nwarm:  %q", start, warmed)
	}
}

func TestWarmLaunchBuildEndedByItsContextLeavesNothingBehind(t *testing.T) {
	app, _ := warmFixture(t)
	testutil.FakeGo(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WarmLaunchBuild(ctx, RunConfig{Build: Config{ApplicationRoot: app, OperatorRoot: "..", Replay: "off"}}); err == nil {
		t.Fatal("a cancelled warm build must fail")
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
}
