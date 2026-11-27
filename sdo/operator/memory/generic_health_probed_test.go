package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// writeProbedMap writes a map of web and components in which one probed user
// operation enters at web and runs through path, over those edges.
func writeProbedMap(t *testing.T, root string, path []string, components ...string) {
	t.Helper()
	writeMap(t, root, append([]string{syntheticNoData}, components...)...)
	file := filepath.Join(root, layout.ArchitectureFile)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	edges := "edges:\n"
	for i := 1; i < len(path); i++ {
		edges += "  - {from: " + path[i-1] + ", to: " + path[i] + ", evidence: [manifest], source: x}\n"
	}
	doc := strings.Replace(string(data), "edges: []\n", edges, 1)
	doc = strings.Replace(doc, "operations: []\n", "operations:\n  - {id: get, writes: false, entry: {service: web, port: 80, method: GET, path: /}, path: ["+strings.Join(path, ", ")+"]}\n", 1)
	doc = strings.Replace(doc, "probes: []\n", "probes:\n  - {scenario: call-get, kind: call, operations: [get]}\n", 1)
	if err := os.WriteFile(file, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A store no user operation reaches cannot explain a failing user request, so
// when the cap bites, stores on a probed operation's path are read first: a
// cache on that path beats an authoritative store off it. Authoritative stores
// still come first within each group.
func TestStoreStateReadersPreferStoresOnProbedPaths(t *testing.T) {
	root := t.TempDir()
	components := []string{
		"  - {name: z-db, workload: StatefulSet/z-db, services: [z-db], holds_data: true, data: {engine: redis, role: authoritative}}",
		"  - {name: a-cache, workload: Deployment/a-cache, services: [a-cache], holds_data: false, data: {engine: redis, role: derived, derived_from: [z-db]}}",
	}
	for i := range MaxStoreStateDetectors {
		components = append(components, fmt.Sprintf("  - {name: idle-%d, workload: StatefulSet/idle-%d, services: [idle-%d], holds_data: true, data: {engine: redis, role: authoritative}}", i, i, i))
	}
	writeProbedMap(t, root, []string{"web", "a-cache", "z-db"}, components...)
	var planned []string
	for _, d := range plannedStoreStateDetectors(root) {
		planned = append(planned, strings.TrimPrefix(d.manifest.ID, StoreStateDetectorPrefix))
	}
	want := []string{"z-db", "a-cache", "idle-0", "idle-1", "idle-2", "idle-3", "idle-4", "idle-5"}
	if strings.Join(planned, " ") != strings.Join(want, " ") {
		t.Fatalf("planned store readers %v, want %v", planned, want)
	}
}

// A client link is on a user path when both its ends are probed: such links
// are dialed before links to components no user operation reaches, even those
// an entry component calls.
func TestClientLinksPreferLinksOnProbedPaths(t *testing.T) {
	root := t.TempDir()
	components := []string{
		"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}",
		"  - {name: kv, workload: StatefulSet/kv, services: [kv], holds_data: true, data: {engine: redis, role: authoritative}}",
	}
	links := "  - {from: api, to: kv, port: 6379, protocol: redis}\n  - {from: web, to: api, port: 80, protocol: tcp}\n"
	for i := range MaxClientLinks {
		components = append(components, fmt.Sprintf("  - {name: idle-%d, workload: Deployment/idle-%d, services: [idle-%d], holds_data: false}", i, i, i))
		links += fmt.Sprintf("  - {from: web, to: idle-%d, port: 80, protocol: tcp}\n", i)
	}
	writeProbedMap(t, root, []string{"web", "api", "kv"}, components...)
	writeLinks(t, root, links)
	var planned []string
	for _, l := range plannedClientLinks(root) {
		planned = append(planned, l.Link.From+">"+l.Link.To)
	}
	want := []string{"web>api", "api>kv", "web>idle-0", "web>idle-1", "web>idle-2", "web>idle-3", "web>idle-4", "web>idle-5"}
	if strings.Join(planned, " ") != strings.Join(want, " ") {
		t.Fatalf("planned client links %v, want %v", planned, want)
	}
}
