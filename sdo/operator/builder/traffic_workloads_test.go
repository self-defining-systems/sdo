package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The controller's synthetic traffic runs from the workload documents the
// generated package embeds; a catalog that carried empty documents would load
// and compile, then probe nothing.
func TestGeneratedTrafficEmbedsEveryWorkloadDocument(t *testing.T) {
	root := t.TempDir()
	for path, content := range map[string]string{
		"traffic/generators/generators.go": readOnlyShop,
		"traffic/workloads/health.yaml":    readOnlyHealth,
		"traffic/workloads/links.yaml":     shopLinks,
	} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := generateTraffic(root, "fixture"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "generated", "traffic.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"health": []byte(`, `health-probe`, `"links": []byte(`, `link-probe`, `generators.Scenarios()`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("generated traffic lacks %q:\n%s", want, data)
		}
	}
}
