package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
)

func TestBuildWorkspaceRejectsManifestThatDisagreesWithItsLocation(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(diagnostics, "detectors", "incidents", "missing-configmap", "manifest.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "id: missing-configmap", "id: elsewhere", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: ".."}); err == nil || !strings.Contains(err.Error(), "disagrees with its location") {
		t.Fatalf("misplaced detector manifest was accepted or rejected for wrong reason: %v", err)
	}
}

func TestBuildWorkspaceRejectsNetworkTrafficGenerator(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	generator := filepath.Join(diagnostics, "traffic", "generators")
	if err := os.MkdirAll(generator, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(generator, "generators.go"), []byte("package generators\nimport \"net/http\"\nvar _ = http.Get\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: ".."})
	if err == nil || !strings.Contains(err.Error(), "may not import") {
		t.Fatalf("networked generator was accepted or rejected for wrong reason: %v", err)
	}
}
