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
<<<<<<< HEAD
=======

func TestBuildWorkspaceRejectsDetectorRegistrationDrift(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(diagnostics, "detectors", "incidents", "missing-configmap", "detector.go")
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
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("healthy baseline gate failed: %v", err)
	}
}

func metricsDetectorSource(rules string) string {
	return `package metricscheck

import (
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

func New() sdk.Detector {
	return metrics.NewThresholdDetector(sdk.DetectorSpec{
		ID: "metrics-check", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:     []sdk.WatchKind{metrics.Watch},
		Interval:    30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}, []metrics.ThresholdRule{` + rules + `})
}
`
}

const metricsDetectorManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: metrics-check
constructor: New
class: health
owner: health_judge
watches:
    - apiVersion: sdo.dev/v1alpha1
      kind: Metrics
interval: 30s
persistence:
    firing: 2
    clearing: 2
batching:
    severity: critical
    debounce: 500ms
possiblePlaybooks:
    - health-objective
originatingCommit: lifecycle-bootstrap
`

func buildMetricsWorkspace(t *testing.T, rules string) error {
	t.Helper()
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(diagnostics, "detectors", "health", "metrics-check")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"manifest.yaml": metricsDetectorManifest, "detector.go": metricsDetectorSource(rules)} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
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
	_, err = workspace.ValidateAndBuild(context.Background())
	return err
}

func TestBuildWorkspaceAcceptsValidMetricsDetectors(t *testing.T) {
	err := buildMetricsWorkspace(t, `{Query: metrics.Query{ID: "errors", Expr: "sum(rate(errors[1m]))"}, Op: metrics.Greater, Threshold: 1}`)
	if err != nil {
		t.Fatalf("valid metrics detector rejected: %v", err)
	}
}

func TestBuildWorkspaceRejectsInvalidMetricsDetectors(t *testing.T) {
	err := buildMetricsWorkspace(t, `{Query: metrics.Query{ID: "errors"}, Op: metrics.Greater, Threshold: 1}`)
	if err == nil || !strings.Contains(err.Error(), "empty expression") {
		t.Fatalf("a detector with an empty metric expression must fail the build tests: %v", err)
	}
}

func TestReadThroughGoCacheDoesNotCopySeedAtStartup(t *testing.T) {
	seed, cache := t.TempDir(), filepath.Join(t.TempDir(), "private-cache")
	if err := os.WriteFile(filepath.Join(seed, "compiled-sdk"), []byte("immutable"), 0400); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	// Startup can invoke Go, but any hidden cache copy fails this test.
	if err := os.WriteFile(filepath.Join(bin, "cp"), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\ntest ! -e \"$GOCACHE/compiled-sdk\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GOCACHEPROG", "/usr/local/bin/sdo-go-cache")
	t.Setenv("SDO_GO_CACHE_SEED", seed)
	t.Setenv("GOCACHE", cache)
	if _, err := (&Workspace{Directory: t.TempDir()}).ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("startup copied immutable seed: %v", err)
	}
	if _, err := os.Stat(cache); !os.IsNotExist(err) {
		t.Fatal("startup materialized the cache before Go requested an entry")
	}
}

func TestReplayOffCompilesProgramsWithoutReadingCorpus(t *testing.T) {
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
		t.Fatal("real compile/import/vet/safety check failed", err)
	}
	notes, err := workspace.MitigationReplayResults()
	if err != nil || len(notes) != 0 {
		t.Fatalf("off ran replay: %v %v", notes, err)
	}
}
>>>>>>> origin/vic-lsh/dev/sdo-improve
