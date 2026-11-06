package memory

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// The alerts detector follows the effective sources of the tree being
// checked: a tree that holds Go's metrics-alerts package from an earlier
// environment (another profile, or a binding since removed) loses it when no
// effective source serves alerts, instead of failing telemetry.unbound on a
// detector the designer did not write and may not edit. A designer-authored
// package with the same id is never removed.
func TestPrepareHealthRemovesItsAlertsDetectorWhenNoSourceServesAlerts(t *testing.T) {
	root := t.TempDir()
	alerts := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, MetricsAlertsDetectorID))
	if err := PrepareHealthWith(root, HealthOptions{AlertSources: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(alerts); err != nil {
		t.Fatalf("alerts detector not installed with an alert source: %v", err)
	}
	if err := PrepareHealthWith(root, HealthOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(alerts); !os.IsNotExist(err) {
		t.Fatalf("Go's alerts detector survived a tree without an alert source: %v", err)
	}

	authored := filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, MetricsAlertsDetectorID))
	if err := os.MkdirAll(filepath.Dir(authored), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(authored, []byte("not a Go-installed manifest\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealthWith(root, HealthOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(authored); err != nil {
		t.Fatalf("a package Go did not install was removed: %v", err)
	}
}
