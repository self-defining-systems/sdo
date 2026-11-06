package memory

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// The published ID list is what the O4 discriminator trusts as Go-owned, so it
// must equal what PrepareHealthWith actually writes.
func TestGenericHealthDetectorIDsMatchWhatPrepareHealthWrites(t *testing.T) {
	root := t.TempDir()
	if err := PrepareHealthWith(root, HealthOptions{AlertSources: true}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "")))
	if err != nil {
		t.Fatal(err)
	}
	var written []string
	for _, entry := range entries {
		written = append(written, entry.Name())
	}
	want := GenericHealthDetectorIDs()
	slices.Sort(written)
	slices.Sort(want)
	if !slices.Equal(written, want) {
		t.Fatalf("PrepareHealthWith wrote %v, GenericHealthDetectorIDs lists %v", written, want)
	}
	if IsGenericHealthDetector("traffic-frontend") || !IsGenericHealthDetector("node-health") {
		t.Fatal("IsGenericHealthDetector does not follow the list")
	}
}

// The ID set validation counts as "Go will install" must equal what
// PrepareHealthWith writes for the same tree, generic and traffic detectors
// alike, with and without the alerts option.
func TestPreparedHealthDetectorIDsMatchWhatPrepareHealthWrites(t *testing.T) {
	probe := []byte("name: probe-call\npurpose: health-probe\n")
	for _, options := range []HealthOptions{{}, {AlertSources: true}} {
		root := t.TempDir()
		dir := filepath.Join(root, layout.TrafficWorkloadsDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "probe-call.yaml"), probe, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := PrepareHealthWith(root, options); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "")))
		if err != nil {
			t.Fatal(err)
		}
		var written []string
		for _, entry := range entries {
			written = append(written, entry.Name())
		}
		slices.Sort(written)
		if got := PreparedHealthDetectorIDs(options, map[string][]byte{"probe-call.yaml": probe}); !slices.Equal(got, written) {
			t.Fatalf("options %+v: prepared IDs %v, PrepareHealthWith wrote %v", options, got, written)
		}
	}
}
