package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

const syntheticDataCheck = `component: order-db
engine: postgres
target: {labels: {app: order-db}}
read: {table: orders, key: id, first: 8}
assert:
  - bounds: {field: total, min: 0}
`

func writeDataCheck(t *testing.T, root, id, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(layout.DataCheckPath(id)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func detectorSource(t *testing.T, root, id string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, id), layout.DetectorFileName))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// Each data check becomes a Go-installed health detector built from the
// file's own text; editing the file replaces it and deleting it removes it.
func TestPrepareHealthInstallsADetectorPerDataCheck(t *testing.T) {
	root := t.TempDir()
	writeDataCheck(t, root, "orders", syntheticDataCheck)
	writeDataCheck(t, root, "orders-late", strings.Replace(syntheticDataCheck, "min: 0", "max: 100", 1)+"interval: 5m\n")
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	source := detectorSource(t, root, "data-orders")
	if !strings.Contains(source, "datacheck.NewDetector(") || !strings.Contains(source, `"orders"`) || !strings.Contains(source, "min: 0") {
		t.Fatalf("detector source:\n%s", source)
	}
	manifest, err := os.ReadFile(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "data-orders-late")))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := layout.ParseDetectorManifest(manifest)
	if err != nil || parsed.Class != "health" || parsed.Interval != "300s" || parsed.OriginatingCommit != "lifecycle-bootstrap" {
		t.Fatalf("manifest = %+v, %v", parsed, err)
	}

	writeDataCheck(t, root, "orders", strings.Replace(syntheticDataCheck, "min: 0", "min: 1", 1))
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if source := detectorSource(t, root, "data-orders"); !strings.Contains(source, "min: 1") {
		t.Fatal("an edited data check kept its old detector")
	}
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(layout.DataCheckPath("orders-late")))); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "data-orders-late") || !detectorExists(root, "data-orders") {
		t.Fatal("the deleted data check's detector must go and the other stay")
	}
}

func TestPrepareHealthRefusesAnInvalidDataCheckAndAnAuthoredNameClash(t *testing.T) {
	root := t.TempDir()
	writeDataCheck(t, root, "orders", strings.Replace(syntheticDataCheck, "first: 8", "first: 8, where: \"pg_sleep(1) IS NULL\"", 1))
	if err := PrepareHealth(root); err == nil || !strings.Contains(err.Error(), "data check orders") {
		t.Fatalf("PrepareHealth = %v", err)
	}

	root = t.TempDir()
	writeDataCheck(t, root, "orders", syntheticDataCheck)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "data-orders"))
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(strings.Replace(string(data), "lifecycle-bootstrap", "judge-1", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err == nil || !strings.Contains(err.Error(), "not Go's data-check detector") {
		t.Fatalf("an authored data-orders detector was overwritten: %v", err)
	}
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(layout.DataCheckPath("orders")))); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil || !detectorExists(root, "data-orders") {
		t.Fatalf("an authored data-* detector was pruned: %v", err)
	}
}
