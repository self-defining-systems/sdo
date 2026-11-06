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
	if err := PrepareHealthWith(root, HealthOptions{Metrics: true}); err != nil {
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
