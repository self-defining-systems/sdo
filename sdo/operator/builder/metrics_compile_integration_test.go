//go:build integration

package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	err = workspace.Check(context.Background())
	return err
}

func TestBuildWorkspaceRejectsInvalidMetricsDetectors(t *testing.T) {
	err := buildMetricsWorkspace(t, `{Query: metrics.Query{ID: "errors"}, Op: metrics.Greater, Threshold: 1}`)
	if err == nil || !strings.Contains(err.Error(), "empty expression") {
		t.Fatalf("a detector with an empty metric expression must fail the build tests: %v", err)
	}
}
