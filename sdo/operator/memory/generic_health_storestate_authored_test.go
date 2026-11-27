package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// designerStoreReader is a designer-written health detector that reads the kv
// store through the SDK under its own ID, interval and import alias.
const designerStoreReader = `package kvread

import (
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	ss "sdo.dev/operator/sdk/storestate"
)

func New() sdk.Detector {
	d, err := ss.NewDetector(ss.Config{
		Spec: sdk.DetectorSpec{
			ID:       "store-cache-kv",
			Class:    sdk.DetectorClassHealth,
			Owner:    sdk.DetectorOwnerHealthJudge,
			Watches:  []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
			Interval: 60 * time.Second,
		},
		Component:    "kv",
		Engine:       ss.Engine("redis"),
		Target:       podread.Target{Workload: "StatefulSet/kv"},
		ReadInterval: 60 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return d
}
`

const designerStoreReaderManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: store-cache-kv
constructor: New
class: health
owner: health_judge
watches:
    - apiVersion: v1
      kind: Pod
interval: 60s
persistence:
    firing: 2
    clearing: 2
batching:
    severity: critical
    debounce: 500ms
possiblePlaybooks:
    - health-objective
originatingCommit: initial
`

func writeDesignerStoreReader(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "store-cache-kv"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{layout.DetectorFileName: designerStoreReader, layout.ManifestFileName: designerStoreReaderManifest} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// A store the designer already reads keeps that one reader: Go installs none
// for it, and an earlier Go reader of it (memory accepted before Go read every
// store) is removed, so the two never declare one pod read differently.
func TestPrepareHealthLeavesADesignerReadStoreToItsReader(t *testing.T) {
	root := t.TempDir()
	writeMap(t, root, syntheticStore, syntheticDerived)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "store-state-kv") {
		t.Fatal("setup: Go did not read kv")
	}
	writeDesignerStoreReader(t, root)
	for range 2 {
		if err := PrepareHealth(root); err != nil {
			t.Fatal(err)
		}
		if detectorExists(root, "store-state-kv") {
			t.Fatal("Go reads kv beside the designer's reader")
		}
		if !detectorExists(root, "store-state-memo") {
			t.Fatal("a store nobody else reads lost its Go reader")
		}
		source, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "store-cache-kv"), layout.DetectorFileName))
		if err != nil || string(source) != designerStoreReader {
			t.Fatalf("the designer's reader changed: %v", err)
		}
	}
	for _, d := range plannedStoreStateDetectors(root) {
		if strings.HasSuffix(d.manifest.ID, "-kv") {
			t.Fatalf("planned %s for a designer-read store", d.manifest.ID)
		}
	}
}
