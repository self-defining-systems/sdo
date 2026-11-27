package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/vantage"
)

func writeLinks(t *testing.T, root, links string) {
	t.Helper()
	dir := filepath.Join(root, layout.TrafficWorkloadsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	doc := "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\nlinks:\n" + links
	if err := os.WriteFile(filepath.Join(dir, "links.yaml"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The prober dials a link from SDO's own Pod, so a loss or partition on the
// client's side of an edge never shows there. Go installs one detector that
// dials every mapped link from inside its calling component's Pods.
func TestPrepareHealthDialsMappedLinksFromTheClient(t *testing.T) {
	root := t.TempDir()
	writeMap(t, root, syntheticStore, syntheticNoData)
	writeLinks(t, root, "  - {from: web, to: kv, port: 6379, protocol: redis}\n  - {from: outside, to: kv, port: 6379, protocol: redis}\n")
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, ClientLinksDetectorID), layout.DetectorFileName))
	if err != nil {
		t.Fatalf("no client-links detector: %v", err)
	}
	for _, want := range []string{`podread.Target{Workload: "Deployment/web"}`, `To: "kv", Port: 6379`, "NewClientLinksDetector", "lifecycle-bootstrap"} {
		if !strings.Contains(string(source), want) {
			t.Errorf("generated source lacks %s:\n%s", want, source)
		}
	}
	if strings.Contains(string(source), `"outside"`) {
		t.Errorf("a link whose caller is no map component has no client Pods to dial from:\n%s", source)
	}
	if !IsGenericHealthDetector(ClientLinksDetectorID) {
		t.Error("the client-links detector is Go's floor")
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatalf("idempotent prepare: %v", err)
	}
	// Without links nothing is dialed and Go's package goes.
	if err := os.Remove(filepath.Join(root, layout.TrafficWorkloadsDir, "links.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	if detectorExists(root, ClientLinksDetectorID) {
		t.Fatal("client-links stayed without links")
	}
}

// Client dials share the vantage load budget with the designer's probes: Go
// dials every link within half of it, links from entry components (the
// exposed web) first.
func TestClientLinksDialEveryLinkEntryFirst(t *testing.T) {
	root := t.TempDir()
	components := []string{syntheticStore, syntheticNoData}
	var links string
	for _, name := range []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"} {
		components = append(components, "  - {name: "+name+", workload: Deployment/"+name+", services: ["+name+"], holds_data: false}")
		links += "  - {from: " + name + ", to: kv, port: 6379, protocol: redis}\n"
	}
	links += "  - {from: web, to: kv, port: 6379, protocol: redis}\n"
	writeMap(t, root, components...)
	path := filepath.Join(root, layout.ArchitectureFile)
	data, _ := os.ReadFile(path)
	data = []byte(strings.Replace(string(data), "# Architecture\n", "# Architecture\n\n- Service/demo/web (source: k8s/web.yaml)\n  - exposure:NodePort\n", 1))
	edges := "edges:\n"
	for _, name := range []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"} {
		edges += "  - {from: web, to: " + name + ", evidence: [manifest], source: x}\n"
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "edges: []\n", edges, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	writeLinks(t, root, links)
	planned := plannedClientLinks(root)
	if len(planned) != 10 || planned[0].Link.From != "web" {
		t.Fatalf("planned %d links, first %+v", len(planned), planned[0])
	}
	for _, l := range planned {
		if l.Interval != 2*vantage.MinInterval {
			t.Fatalf("ten links in a budget of %d dial every %s, got %+v", ClientLinkLoad, 2*vantage.MinInterval, l)
		}
	}
}
