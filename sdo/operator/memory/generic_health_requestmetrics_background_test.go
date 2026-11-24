package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// Both internal operations and the job metric of a legacy HTTP-shaped
// operation count as background. Real user requests remain foreground.
func TestRequestMetricsEntryOfBackgroundWorkIsMarked(t *testing.T) {
	root := t.TempDir()
	writeEntriesMap(t, root, `operations:
  - {id: list-items, entry: {service: web, port: 80, method: GET, path: /items}, path: [web]}
  - {id: send-mail, entry: {service: worker, port: 80, method: GET, path: mailers}, path: [worker]}
  - {id: archive, entry: {service: batch, path: archive}, path: [batch]}
entries:
  - {service: web, metric: http_requests_total, labels: {route: route, method: method, status: code}, errors: "5.."}
  - {service: worker, metric: jobs_total, labels: {route: queue, status: code}, errors: "5.."}
  - {service: batch, metric: executions_total, labels: {route: batch, status: code}, errors: "5.."}
`)
	cfg, ok := plannedRequestMetrics(root)
	if !ok {
		t.Fatal("not planned")
	}
	got := map[string]bool{}
	for _, e := range cfg.Entries {
		got[e.Service] = e.Background
	}
	if got["web"] || !got["worker"] || !got["batch"] {
		t.Fatalf("background by service = %v, want worker and batch only", got)
	}
}

func TestWriteSchemaRefreshesInstalledRequestMetricsOnCurrentSchema(t *testing.T) {
	root := t.TempDir()
	writeEntriesMap(t, root, strings.Replace(syntheticRequestMap, "http_requests_total", "worker_jobs_total", 1))
	if err := WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, RequestMetricsDetectorID), layout.DetectorFileName)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("upkeep must leave missing detector absent, stat = %v", err)
	}
	if err := PrepareHealthWith(root, HealthOptions{RequestMetrics: true}); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The old generator accepted the HTTP-shaped operation as foreground.
	_, body, _ := strings.Cut(string(current), "\n")
	legacy := withGeneratedMarker([]byte(strings.Replace(body, "Background: true", "Background: false", 1)))
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	updated, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(updated), "Background: true") {
		t.Fatalf("expected legacy detector to be refreshed, read error %v: %s", err, updated)
	}
	if err := WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(path)
	if err != nil || string(again) != string(updated) {
		t.Fatalf("second upkeep changed the detector (read error %v)", err)
	}
}
