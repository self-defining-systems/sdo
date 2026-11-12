package memory_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// A Go rewrite of the inventory copies the designer's application map verbatim.
func TestWriteArchitectureCarriesTheApplicationMapVerbatim(t *testing.T) {
	checkout := t.TempDir()
	if err := memory.WriteArchitecture(checkout, "shop", "c1", "# Manifest inventory\n\n- Deployment/shop/web (source: web.yaml)\n"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(checkout, layout.ArchitectureFile)
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	region := appmap.BeginMarker + "\n```yaml\nversion: 1\n# hand-written, spacing   kept\n```\n" + appmap.EndMarker + "\n"
	if err := os.WriteFile(path, append(first, []byte("\n"+region)...), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := memory.WriteArchitecture(checkout, "shop", "c2", "# Manifest inventory\n\n- Deployment/shop/web2 (source: web.yaml)\n"); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := appmap.Find(string(second))
	if err != nil || !found || got.Block != region {
		t.Fatalf("region after the rewrite = %q (%v, %v)", got.Block, found, err)
	}
	if !strings.Contains(string(second), "generated_at_commit: c2") || !strings.Contains(string(second), "web2") || strings.Contains(appmap.Outside(string(second)), "hand-written") {
		t.Fatalf("inventory was not regenerated outside the region:\n%s", second)
	}
	if text, err := memory.ReadArchitecture(checkout); err != nil || !strings.Contains(text, "spacing   kept") {
		t.Fatalf("agents do not see the map in the architecture text: %v\n%s", err, text)
	}
}

// The designer's brief lists the user-path components no probed user
// operation covers, so coverage gaps drive probe design instead of being found
// only after acceptance.
func TestDesignerBriefListsUncoveredUserPathComponents(t *testing.T) {
	checkout := t.TempDir()
	if err := memory.WriteArchitecture(checkout, "shop", "c1", "# Manifest inventory\n\n- Deployment/shop/web (source: web.yaml)\n"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(checkout, layout.ArchitectureFile)
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	region := appmap.BeginMarker + "\n```yaml\n" + `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}
  - {name: blobs, workload: StatefulSet/blobs, services: [blobs], holds_data: true}
edges:
  - {from: web, to: db, evidence: [manifest], source: ConfigMap/web key dsn}
  - {from: web, to: blobs, evidence: [manifest], source: ConfigMap/web key blob-url}
operations:
  - id: sign-in
    writes: false
    entry: {service: web, port: 80, method: GET, path: /sign-in}
    path: [web, db]
probes:
  - {scenario: call-sign-in, kind: call, operations: [sign-in]}
` + "```\n" + appmap.EndMarker + "\n"
	if err := os.WriteFile(path, append(first, []byte("\n"+region)...), 0o644); err != nil {
		t.Fatal(err)
	}
	brief := memory.CoverageObligations(checkout)
	if !strings.Contains(brief, "- blobs\n") || strings.Contains(brief, "- db\n") || strings.Contains(brief, "- web\n") || !strings.Contains(brief, "unprobed") {
		t.Fatalf("brief does not list exactly the uncovered component:\n%s", brief)
	}
	if text, err := memory.ReadArchitecture(checkout); err != nil || strings.Contains(text, "Probe coverage") {
		t.Fatalf("the architecture text itself stays what the file holds: %v", err)
	}
	covered := strings.Replace(region, "path: [web, db]", "path: [web, db, blobs]", 1)
	if err := os.WriteFile(path, append(first, []byte("\n"+covered)...), 0o644); err != nil {
		t.Fatal(err)
	}
	if brief := memory.CoverageObligations(checkout); brief != "" {
		t.Fatalf("a fully covered map has no obligations:\n%s", brief)
	}
}
