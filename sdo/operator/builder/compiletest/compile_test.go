//go:build integration

// Package compiletest keeps the real compiler smoke off builder's serial test path.
package compiletest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/builder"
	"strings"
	"testing"
)

func TestBuildWorkspaceCompilesApplicationDetectors(t *testing.T) {
	repository := filepath.Join("..", "..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	operatorRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := builder.BuildWorkspace(context.Background(), builder.Config{ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	generated, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", "detectors.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), `d0 "fixture-diagnostics/detectors/incidents/missing-configmap"`) {
		t.Fatalf("generated detector import missing: %s", generated)
	}
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("generated workspace does not compile: %v", err)
	}
	if !workspace.HasProber {
		t.Fatal("an application without traffic generators still needs routing probes")
	}
	if output, err := exec.Command(filepath.Join(workspace.Directory, "sdo-prober"), "--check").CombinedOutput(); err != nil {
		t.Fatalf("generated no-traffic prober cannot start: %v: %s", err, output)
	}
}
