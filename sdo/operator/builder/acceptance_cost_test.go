package builder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// Acceptance compiles every package, vets and executes required tests. Only
// the launch path needs a retained controller artifact after those gates.
func assertAcceptanceDoesNotRelinkLaunchArtifacts(t *testing.T, workspace *Workspace) {
	t.Helper()
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
		if err := workspace.CheckAcceptance(context.Background()); err != nil {
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

// BenchmarkPublicationPhases compares the same generated publication fixture
// with and without the informational pass. Use -benchtime=1x for phase seconds.
func BenchmarkPublicationPhases(b *testing.B) {
	for _, launch := range []bool{false, true} {
		for _, advisory := range []bool{true, false} {
			name := "validator/after"
			if launch {
				name = "relaunch/after"
			}
			if advisory {
				name = strings.Replace(name, "after", "before", 1)
			}
			b.Run(name, func(b *testing.B) {
				for b.Loop() {
					started := time.Now()
					w := acceptanceFixture(b)
					b.ReportMetric(time.Since(started).Seconds(), "staging-seconds")
					started = time.Now()
					var err error
					if launch {
						if advisory {
							_, err = w.ValidateAndBuild(context.Background())
						} else {
							_, err = w.BuildForLaunch(context.Background())
						}
					} else {
						if advisory {
							err = w.Check(context.Background())
						} else {
							err = w.CheckAcceptance(context.Background())
						}
					}
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(time.Since(started).Seconds(), "phase-seconds")
					for _, stage := range w.StageTimings {
						b.ReportMetric(stage.Duration.Seconds(), strings.ReplaceAll(strings.ReplaceAll(stage.Stage, w.Directory, "WORKSPACE"), " ", "_")+"-seconds")
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
