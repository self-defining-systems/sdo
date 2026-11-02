package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CopyPortProgram stages the hand-written builder fixture in another test app.
// Only its local library import changes to match that app's diagnostics module.
func CopyPortProgram(t *testing.T, appRoot, fixtureMemory string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(appRoot, ".sdo/diagnostics/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 || fields[0] != "module" {
		t.Fatal("test app requires an accepted diagnostics module")
	}
	module := fields[1]
	for _, name := range []string{
		"playbooks/service-targetport-mismatch/README.md",
		"playbooks/service-targetport-mismatch/diagnose.go",
		"playbooks/service-targetport-mismatch/mitigate.go",
		"lib/ports/ports.go",
		"diagnostics/detectors/incidents/routing-relations/detector.go",
		"diagnostics/detectors/incidents/routing-relations/manifest.yaml",
	} {
		data, err := os.ReadFile(filepath.Join(fixtureMemory, name))
		if err != nil {
			t.Fatal(err)
		}
		data = []byte(strings.ReplaceAll(string(data), "fixture-diagnostics/lib/ports", module+"/lib/ports"))
		target := filepath.Join(appRoot, ".sdo", name)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
