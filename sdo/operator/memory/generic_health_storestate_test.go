package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// writeMap writes an arch.md whose application map holds components.
func writeMap(t *testing.T, root string, components ...string) {
	t.Helper()
	doc := "# Architecture\n\n" + appmap.BeginMarker + "\n```yaml\nversion: 1\ncomponents:\n" + strings.Join(components, "\n") + "\nedges: []\noperations: []\nprobes: []\n```\n" + appmap.EndMarker + "\n"
	path := filepath.Join(root, layout.ArchitectureFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

const (
	syntheticStore   = "  - {name: kv, workload: StatefulSet/kv, services: [kv], holds_data: true, data: {engine: redis, role: authoritative}}"
	syntheticOther   = "  - {name: docs, workload: StatefulSet/docs, services: [docs], holds_data: true, data: {engine: other, role: authoritative}}"
	syntheticNoData  = "  - {name: web, workload: Deployment/web, services: [web], holds_data: false}"
	syntheticDerived = "  - {name: memo, workload: Deployment/memo, services: [memo], holds_data: false, data: {engine: redis, role: derived, derived_from: [kv]}}"
)

// Go installs one store-state detector per map component whose engine SDO can
// read, targeting the component's workload; no LLM authors it.
func TestPrepareHealthInstallsStoreStateFromTheMap(t *testing.T) {
	root := t.TempDir()
	writeMap(t, root, syntheticStore, syntheticOther, syntheticNoData, syntheticDerived)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]bool{"store-state-kv": true, "store-state-memo": true, "store-state-docs": false, "store-state-web": false} {
		if detectorExists(root, id) != want {
			t.Errorf("%s installed = %v, want %v", id, !want, want)
		}
	}
	source, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "store-state-kv"), layout.DetectorFileName))
	if err != nil {
		t.Fatal(err)
	}
	// Go's reader treats a store it never read healthy as a coverage gap, so
	// an unobservable store never stalls acceptance or recovery.
	for _, want := range []string{`"StatefulSet/kv"`, `storestate.Engine("redis")`, `"kv",`, "lifecycle-bootstrap", "GapWithoutBaseline: true"} {
		if !strings.Contains(string(source), want) {
			t.Errorf("generated source lacks %s:\n%s", want, source)
		}
	}
	manifest, err := os.ReadFile(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "store-state-kv")))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := layout.ParseDetectorManifest(manifest)
	if err != nil || parsed.OriginatingCommit != "lifecycle-bootstrap" {
		t.Fatalf("manifest = %+v, %v", parsed, err)
	}
	if !IsGenericHealthDetector("store-state-kv") {
		t.Error("a Go-installed store-state detector is not Go's floor")
	}
	// A second pass over the same tree is a no-op, as for every generic package.
	if err := PrepareHealth(root); err != nil {
		t.Fatalf("idempotent prepare: %v", err)
	}
}

// A component that leaves the map, or loses its data role, takes its
// Go-installed detector with it.
func TestPrepareHealthRemovesStoreStateOfAComponentNoLongerInTheMap(t *testing.T) {
	root := t.TempDir()
	writeMap(t, root, syntheticStore, syntheticDerived)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	writeMap(t, root, syntheticStore)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "store-state-memo") || !detectorExists(root, "store-state-kv") {
		t.Fatal("store-state detectors do not follow the map")
	}
	if err := os.Remove(filepath.Join(root, layout.ArchitectureFile)); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, "store-state-kv") {
		t.Fatal("store-state-kv outlived the map")
	}
}

// Store reads share the pod-read budget with the designer's own reads, so Go
// installs at most MaxStoreStateDetectors, authoritative stores first.
func TestPrepareHealthCapsStoreStateDetectors(t *testing.T) {
	root := t.TempDir()
	var components []string
	for i := range MaxStoreStateDetectors + 2 {
		components = append(components, fmt.Sprintf("  - {name: copy-%d, workload: Deployment/copy-%d, services: [copy-%d], holds_data: false, data: {engine: redis, role: derived, derived_from: [kv]}}", i, i, i))
	}
	components = append(components, syntheticStore)
	writeMap(t, root, components...)
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if !detectorExists(root, "store-state-kv") {
		t.Fatal("the authoritative store lost its reader to derived copies")
	}
	entries, err := os.ReadDir(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "")))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "store-state-") {
			count++
		}
	}
	if count != MaxStoreStateDetectors {
		t.Fatalf("installed %d store-state detectors, want %d", count, MaxStoreStateDetectors)
	}
}
