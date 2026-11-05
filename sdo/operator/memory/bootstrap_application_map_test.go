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
