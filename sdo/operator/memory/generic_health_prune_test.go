package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func writeProbeWorkload(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, layout.TrafficWorkloadsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "name: " + name + "\npurpose: health-probe\n"
	if err := os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func detectorExists(root, id string) bool {
	_, err := os.Stat(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, id)))
	return err == nil
}

// Deleting a probe workload must delete the Go-installed detector that
// consumed it; a stale package fails the consume check with no way to repair it.
func TestPrepareHealthRemovesTrafficDetectorOfDeletedWorkload(t *testing.T) {
	root := t.TempDir()
	writeProbeWorkload(t, root, "probe-a")
	writeProbeWorkload(t, root, "probe-b")
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "traffic-probe-a") || !detectorExists(root, "traffic-probe-b") {
		t.Fatal("both traffic detectors should be installed")
	}
	if err := os.Remove(filepath.Join(root, layout.TrafficWorkloadsDir, "probe-b.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "traffic-probe-b") {
		t.Fatal("traffic-probe-b outlived its workload")
	}
	if !detectorExists(root, "traffic-probe-a") {
		t.Fatal("traffic-probe-a must stay")
	}
}

func TestPrepareHealthKeepsAuthoredTrafficNamedDetector(t *testing.T) {
	root := t.TempDir()
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	// Copy a generated package under a traffic- name and mark it authored.
	src := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "node-health"))
	dst := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "traffic-custom"))
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dst, layout.ManifestFileName)
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "lifecycle-bootstrap", "judge-1", 1))
	if err := os.WriteFile(manifest, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "traffic-custom") {
		t.Fatal("an authored traffic-named detector was pruned")
	}
}

// A slow link is a warning finding, so the installed link detector batches
// warnings: slow links still open incidents.
func TestPrepareHealthBatchesLinkDetectorWarnings(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, layout.TrafficWorkloadsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "links.yaml"), []byte("name: links\npurpose: link-probe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "traffic-links")))
	if err != nil {
		t.Fatal(err)
	}
	var files strings.Builder
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "traffic-links"), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files.Write(data)
	}
	if !strings.Contains(files.String(), "sdk.SeverityWarning") || strings.Contains(files.String(), "sdk.SeverityCritical") || !strings.Contains(files.String(), "severity: warn") {
		t.Fatalf("link detector does not batch warnings:\n%s", files.String())
	}
}

// A tree accepted before runtime-crashes was retired still carries its
// Go-installed package; preparation removes it.
func TestPrepareHealthRemovesRetiredRuntimeCrashesDetector(t *testing.T) {
	root := t.TempDir()
	if err := writeGenericDetector(root, genericManifest("runtime-crashes", "30s", nil), "package synthetic\n"); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "runtime-crashes") {
		t.Fatal("fixture package was not written")
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "runtime-crashes") {
		t.Fatal("the retired runtime-crashes package survived preparation")
	}
}

func writeAuthoredDetector(t *testing.T, root, id string) {
	t.Helper()
	manifest := genericManifest(id, "30s", nil)
	manifest.OriginatingCommit = "initial"
	data, err := layout.MarshalDetectorManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "detector.go"), []byte("package synthetic\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A schema migration drops the Go-installed generic detector Go no longer
// plans and nothing else: current generic detectors and an authored detector
// with a similar name stay.
func TestWriteSchemaMigrationRemovesOnlyStrandedGenericDetectors(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, layout.Dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, layout.SchemaVersionFile), []byte("7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"runtime-crashes", "synthetic-retired"} {
		if err := writeGenericDetector(root, genericManifest(retired, "30s", nil), "package synthetic\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeGenericDetector(root, genericManifest("warning-events", "30s", nil), "package synthetic\n"); err != nil {
		t.Fatal(err)
	}
	writeAuthoredDetector(t, root, "runtime-crashes-authored")
	if err := WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"runtime-crashes", "synthetic-retired"} {
		if detectorExists(root, id) {
			t.Fatalf("stranded generic detector %s survived migration", id)
		}
	}
	for _, id := range []string{"warning-events", "runtime-crashes-authored"} {
		if !detectorExists(root, id) {
			t.Fatalf("detector %s was removed by migration", id)
		}
	}
}

// An authored detector that reuses a retired generic ID is not Go's and stays.
func TestPruneStrandedKeepsAuthoredDetectorWithRetiredID(t *testing.T) {
	root := t.TempDir()
	writeAuthoredDetector(t, root, "runtime-crashes")
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "runtime-crashes") {
		t.Fatal("an authored detector was removed")
	}
}
