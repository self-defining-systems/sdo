//go:build integration

package builder

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// The accepted revision's bindings reach the generated host's source and the
// host binary builds with them, through the same workspace the controller
// stages for an accepted revision.
func TestMemoryHostBuildsWithTheAcceptedTelemetrySources(t *testing.T) {
	app := t.TempDir()
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := os.MkdirAll(filepath.Join(diagnostics, "telemetry"), 0o755); err != nil {
		t.Fatal(err)
	}
	want := "version: 1\nsources:\n  - id: prom\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: http://prom.sdo-ctl.svc:9090\n"
	if err := os.WriteFile(filepath.Join(diagnostics, "telemetry", "sources.yaml"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: "..", Replay: "off"})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if got := compiledTelemetrySources(t, workspace.Directory); got != want {
		t.Fatalf("workspace compiled %q, want %q", got, want)
	}
	if _, err := workspace.BuildMemoryHost(context.Background()); err != nil {
		t.Fatal(err)
	}
}
