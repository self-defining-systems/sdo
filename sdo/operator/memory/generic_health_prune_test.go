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
