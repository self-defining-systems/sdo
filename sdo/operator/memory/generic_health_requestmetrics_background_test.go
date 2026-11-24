package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func TestRequestMetricsEntryOfBackgroundWorkIsMarked(t *testing.T) {
	root := t.TempDir()
	writeEntriesMap(t, root, `operations:
  - {id: list-items, entry: {service: web, port: 80, method: GET, path: /items}, path: [web]}
  - {id: send-mail, entry: {service: web, path: mailers}, path: [web]}
  - {id: archive, entry: {service: batch, path: archive}, path: [batch]}
entries:
  - {service: web, metric: uwsgi_worker_requests_total, labels: {route: route, method: method, status: code}, errors: "5.."}
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
	if len(got) != 2 || got["web"] || !got["batch"] {
		t.Fatalf("background by service = %v, want foreground web and background batch", got)
	}
}

func TestWriteSchemaPreservesAcceptedRequestMetrics(t *testing.T) {
	root := t.TempDir()
	writeEntriesMap(t, root, syntheticRequestMap)
	if err := WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealthWith(root, HealthOptions{RequestMetrics: true}); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, RequestMetricsDetectorID))
	original := map[string]string{}
	for _, file := range []string{layout.DetectorFileName, layout.ManifestFileName} {
		path := filepath.Join(dir, file)
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		original[path] = string(before)
	}
	for _, contents := range []string{
		strings.Replace(syntheticRequestMap, "http_requests_total", "other_requests_total", 1),
		"operations: []",
		"operations: [",
	} {
		writeEntriesMap(t, root, contents)
		if err := WriteSchema(root); err != nil {
			t.Fatal(err)
		}
		for path, before := range original {
			after, err := os.ReadFile(path)
			if err != nil || string(after) != before {
				t.Errorf("schema upkeep changed %s with map %q (read error %v)", filepath.Base(path), contents, err)
			}
		}
	}
}
