package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
)

func TestOnboardingInitScaffoldsGoalJourneysWithoutReplacingAuthoredFiles(t *testing.T) {
	root := t.TempDir()
	arch := "# Go-owned inventory\n\n" + appmap.BeginMarker + "\n```yaml\nversion: 1\ncomponents:\n  - {name: web, workload: Deployment/web, services: [front], holds_data: false}\nedges: []\noperations: []\nprobes: []\n```\n" + appmap.EndMarker + "\n"
	goal := "---\nschema_version: 1\njourneys:\n  - {id: browse, what: Browse items}\n  - {id: submit, what: Submit a form}\n---\nKeep the application healthy.\n"
	for path, content := range map[string]string{".sdo/arch.md": arch, ".sdo/goal.md": goal} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	entry := []initEntry{{service: "front", port: 8080}}
	if err := writeOnboardingScaffold(root, entry, ""); err != nil {
		t.Fatal(err)
	}
	_, m, mapErr, err := journeyGoalAndMap(root)
	if err != nil || mapErr != "" || len(m.Operations) != 2 || len(m.Probes) != 2 || len(m.Components) != 1 {
		t.Fatalf("map = %+v, errors = %s, %v", m, mapErr, err)
	}
	for i, id := range []string{"browse", "submit"} {
		op, probe := m.Operations[i], m.Probes[i]
		if op.Journey != id || op.Entry.Service != "front" || op.Entry.Port != 8080 ||
			len(op.Path) != 1 || op.Path[0] != "web" || len(probe.Operations) != 1 || probe.Operations[0] != op.ID {
			t.Fatalf("journey %s: operation %+v, probe %+v", id, op, probe)
		}
	}
	source, err := os.ReadFile(filepath.Join(root, initGenerator))
	if err != nil || !strings.Contains(string(source), `"journey-1"`) || !strings.Contains(string(source), `"journey-2"`) {
		t.Fatalf("generator = %s, error = %v", source, err)
	}
	testArch, err := os.ReadFile(filepath.Join(root, ".sdo/arch.md"))
	if err != nil || appmap.Outside(string(testArch)) != appmap.Outside(arch) {
		t.Fatalf("architecture = %s, error = %v", testArch, err)
	}
	if err := writeOnboardingScaffold(root, entry, ""); err == nil {
		t.Fatal("init accepted a second write to an authored checkout")
	}
	if second, err := os.ReadFile(filepath.Join(root, initGenerator)); err != nil || string(second) != string(source) {
		t.Fatalf("init replaced the first scaffold: %s, %v", second, err)
	}
}

func TestOnboardingInitHelpAndAmbiguousEntry(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := RunOnboarding(context.Background(), []string{"init", "--help"}, &out, &stderr); code != exitClean ||
		!strings.Contains(out.String(), "--entry SERVICE:PORT") || stderr.Len() != 0 {
		t.Fatalf("code = %d, help = %q, stderr = %q", code, out.String(), stderr.String())
	}
	ports := []initEntry{{service: "web", port: 80}, {service: "api", port: 8080}}
	if _, err := selectInitEntry(ports, ""); err == nil || !strings.Contains(err.Error(), "api:8080, web:80") {
		t.Fatalf("ambiguous entry diagnostic: %v", err)
	}
	if entry, err := selectInitEntry(ports, "api:8080"); err != nil || entry != ports[1] {
		t.Fatalf("selected %v, error = %v", entry, err)
	}
}
