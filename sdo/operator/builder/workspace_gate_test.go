package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"
)

func TestBuildWorkspaceRunsOptInHealthyBaseline(t *testing.T) {
	testutil.FakeGo(t, testutil.GoRule{Verb: "test", Covers: "generated", Marker: "generated/healthy_baseline_test.go", Output: "synthetic healthy baseline firing"})
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	baseline := filepath.Join(app, "baseline")
	if err := os.MkdirAll(baseline, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(baseline, "healthy.json"), capturefixture.FromView(t, `{"namespace":"shop","services":[],"pods":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: operatorRoot, HealthyBaseline: baseline})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated", "healthy_baseline_test.go")); err != nil {
		t.Fatal(err)
	}
	// The generated healthy-baseline test is part of the acceptance stages: a
	// firing detector there rejects the workspace.
	if err := workspace.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "synthetic healthy baseline firing") {
		t.Fatalf("healthy baseline gate did not reject a firing detector: %v", err)
	}
}

func TestReplayOffCompilesProgramsWithoutReadingCorpus(t *testing.T) {
	testutil.FakeGo(t)
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app")
	if err := copyTree(context.Background(), source, app); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(app, ".sdo/corpus/targetport-mismatch/labels.json"), []byte("malformed synthetic labels"), 0644); err != nil {
		t.Fatal(err)
	}
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo/diagnostics"), OperatorRoot: operator, Replay: "off", HealthyBaseline: filepath.Join(app, "missing-baseline")}
	workspace, err := BuildWorkspace(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := os.Stat(filepath.Join(workspace.Directory, "corpus")); !os.IsNotExist(err) {
		t.Fatalf("off staged corpus: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated/programs_test.go")); !os.IsNotExist(err) {
		t.Fatalf("off generated replay: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated/program_declarations_test.go")); err != nil {
		t.Fatal("registration safety was removed", err)
	}
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatal("check failed", err)
	}
	notes, err := workspace.MitigationReplayResults()
	if err != nil || len(notes) != 0 {
		t.Fatalf("off ran replay: %v %v", notes, err)
	}
}
