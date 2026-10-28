package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const readOnlyShop = `package generators

import "sdo.dev/operator/sdk/traffic"

func Scenarios() traffic.Catalog {
	return traffic.Catalog{{
		ID: "browse", Target: traffic.Target{Service: "frontend", Port: 8080},
		DependsOn: []string{"frontend", "catalog"}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "list", Endpoint: traffic.GET("/products", nil)}},
	}}
}
`

const readOnlyHealth = `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: health
purpose: health-probe
ratePerSecond: 2
scenarios:
  - id: browse
`

const shopLinks = `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: links
purpose: link-probe
links:
  - {from: frontend, to: catalog, port: 8080}
  - {from: checkout, to: payment, port: 8080}
`

// A health probe that never crosses a declared dependency edge cannot see a
// fault that answers connections but fails requests on that edge. The check
// says so without rejecting the memory.
func TestValidateNotesDependencyEdgesNoHealthScenarioDrives(t *testing.T) {
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	for path, content := range map[string]string{
		"traffic/generators/shop.go":   readOnlyShop,
		"traffic/workloads/health.yaml": readOnlyHealth,
		"traffic/workloads/links.yaml":  shopLinks,
	} {
		full := filepath.Join(diagnostics, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("an undriven edge must not reject memory: %v", err)
	}
	gaps, err := workspace.TrafficCoverageGaps()
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(gaps, "\n")
	if !strings.Contains(joined, "traffic edge checkout -> payment:8080 is not driven") || strings.Contains(joined, "frontend -> catalog") {
		t.Fatalf("want exactly the undriven checkout -> payment edge noted, got %q", gaps)
	}
}
