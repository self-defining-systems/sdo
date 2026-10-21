package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildWorkspaceCompilesApplicationDetectors(t *testing.T) {
	repository := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	generated, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", "detectors.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), `d0 "fixture-diagnostics/detectors/missing_configmap"`) {
		t.Fatalf("generated detector import missing: %s", generated)
	}
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("generated workspace does not compile: %v", err)
	}
}

func TestValidateAndBuildSeedsGoCacheBeforeProgramCompilation(t *testing.T) {
	seed := t.TempDir()
	cache := filepath.Join(t.TempDir(), "go-cache")
	if err := os.WriteFile(filepath.Join(seed, "compiled-sdk"), []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	goCommand := filepath.Join(bin, "go")
	if err := os.WriteFile(goCommand, []byte("#!/bin/sh\ntest -f \"$GOCACHE/compiled-sdk\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SDO_GO_CACHE_SEED", seed)
	t.Setenv("GOCACHE", cache)
	workspace := &Workspace{Directory: t.TempDir()}

	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("program build did not use the seeded SDK cache: %v", err)
	}
}

func TestValidateAndBuildUsesColdCacheWhenImageHasNoSeed(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "go-cache")
	bin := t.TempDir()
	goCommand := filepath.Join(bin, "go")
	if err := os.WriteFile(goCommand, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SDO_GO_CACHE_SEED", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("GOCACHE", cache)

	if _, err := (&Workspace{Directory: t.TempDir()}).ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("a pre-cache image cannot run a cold program build: %v", err)
	}
}

func TestBuildWorkspaceAcceptsOpaqueDiagnosticsProjection(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	projection := filepath.Join(app, "projection", "bundle")
	if err := copyTree(context.Background(), source, projection); err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: projection, OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("projected diagnostics did not compile: %v", err)
	}
}

func TestBuildWorkspaceRejectsEscapingDetectorPackage(t *testing.T) {
	app := t.TempDir()
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := os.MkdirAll(diagnostics, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\ndetectors:\n - id: bad\n   package: ../outside\n   constructor: New\n   class: incident\n   owner: responder\n   originatingIncident: incident\n   originatingCommit: commit\n   possiblePlaybooks: []\n   interval: 1s\n   persistence:\n     firing: 1\n     clearing: 1\n   batching:\n     severity: warn\n     debounce: 0\n"
	if err := os.WriteFile(filepath.Join(diagnostics, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: ".."}); err == nil || !strings.Contains(err.Error(), "escapes diagnostics") {
		t.Fatalf("escaping detector package was accepted or rejected for wrong reason: %v", err)
	}
}

func TestBuildWorkspaceCompilesIsolatedTrafficProber(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "benchmarks", "sregym", "experiments", "assurance", "seeds", "hotel_reservation_30e023d", "operational_memory", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if !workspace.HasProber {
		t.Fatal("traffic generators should create a prober")
	}
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("traffic prober did not build: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "sdo-prober")); err != nil {
		t.Fatal(err)
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

func TestBuildWorkspaceRejectsDetectorRegistrationDrift(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(diagnostics, "detectors", "missing_configmap", "detector.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(data), `ID:                  "missing-configmap"`, `ID:                  "wrong-id"`, 1)
	if err := os.WriteFile(path, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(context.Background()); err == nil || !strings.Contains(err.Error(), "registration mismatch") {
		t.Fatalf("detector drift was accepted or rejected for wrong reason: %v", err)
	}
}

func TestBuildWorkspaceRunsOptInHealthyBaseline(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(baseline, "healthy.json"), []byte(`{"namespace":"shop","services":[],"pods":[]}`), 0o644); err != nil {
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
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("healthy baseline gate failed: %v", err)
	}
}
