package builder

import (
	"context"
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
