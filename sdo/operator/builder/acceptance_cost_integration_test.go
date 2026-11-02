//go:build integration

package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Acceptance compiles every package, vets and executes required tests. Only
// the launch path needs a retained controller artifact after those gates.
func assertAcceptanceDoesNotRelinkLaunchArtifacts(t *testing.T, workspace *Workspace) {
	t.Helper()
	for _, name := range []string{"sdo-controller", "sdo-memory-host"} {
		if _, err := os.Stat(filepath.Join(workspace.Directory, name)); !os.IsNotExist(err) {
			t.Fatalf("acceptance unnecessarily emitted launch artifact %s: %v", name, err)
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
