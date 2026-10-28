package validate

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestNormalizeCapturedCorpusStoresEachSnapshotOnce(t *testing.T) {
	root := t.TempDir()
	fixed, open := `{"namespace":"app","object_snapshot_version":2}`, `{"namespace":"app","object_snapshot_version":2,"objects":[]}`
	files := map[string]string{
		// A writer copied fixed.json into open.json and healthy.json.
		".sdo/corpus/copied/open.json":    fixed,
		".sdo/corpus/copied/fixed.json":   fixed,
		".sdo/corpus/copied/healthy.json": fixed,
		// Distinct snapshots stay.
		".sdo/corpus/distinct/open.json":    open,
		".sdo/corpus/distinct/fixed.json":   fixed,
		".sdo/corpus/distinct/healthy.json": open,
		// Unchanged accepted cases are never touched.
		".sdo/corpus/accepted/fixed.json":   fixed,
		".sdo/corpus/accepted/healthy.json": fixed,
	}
	var paths []string
	for path, data := range files {
		write(t, root, path, data)
		if filepath.Base(filepath.Dir(path)) != "accepted" {
			paths = append(paths, path)
		}
	}
	removed, err := NormalizeCapturedCorpus(root, paths)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(removed)
	if want := []string{".sdo/corpus/copied/healthy.json", ".sdo/corpus/copied/open.json"}; !slices.Equal(removed, want) {
		t.Fatalf("removed = %v, want %v", removed, want)
	}
	for path := range files {
		_, err := os.Stat(filepath.Join(root, path))
		if gone := slices.Contains(removed, path); gone != os.IsNotExist(err) {
			t.Fatalf("%s: removed=%v stat=%v", path, gone, err)
		}
	}
}
