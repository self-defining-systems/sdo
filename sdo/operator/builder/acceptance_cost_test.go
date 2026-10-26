package builder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func acceptanceFixture(t testing.TB) *Workspace {
	t.Helper()
	repository := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	workspace, err := BuildWorkspace(context.Background(), Config{
		ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), OperatorRoot: "..",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	return workspace
}

// Acceptance compiles every package, vets and executes its tests/replay. Only
// the launch path needs a retained controller artifact after those gates.
func TestAcceptanceCheckDoesNotRelinkLaunchArtifacts(t *testing.T) {
	workspace := acceptanceFixture(t)
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sdo-controller"} {
		if _, err := os.Stat(filepath.Join(workspace.Directory, name)); !os.IsNotExist(err) {
			t.Fatalf("acceptance unnecessarily emitted launch artifact %s: %v", name, err)
		}
	}
	if output, err := exec.Command(filepath.Join(workspace.Directory, "sdo-prober"), "--check").CombinedOutput(); err != nil {
		t.Fatalf("acceptance lost its portable prober build: %v: %s", err, output)
	}
}

// Use the same real model-free fixture before and after a builder change.
// Includes staging and all acceptance gates, with the host's warm Go cache.
func BenchmarkAcceptanceCheck(b *testing.B) {
	for b.Loop() {
		workspace := acceptanceFixture(b)
		if err := workspace.Check(context.Background()); err != nil {
			b.Fatal(err)
		}
		for _, stage := range workspace.StageTimings {
			b.ReportMetric(stage.Duration.Seconds(), strings.ReplaceAll(strings.ReplaceAll(stage.Stage, workspace.Directory, "WORKSPACE"), " ", "_")+"-seconds")
		}
		if err := workspace.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
