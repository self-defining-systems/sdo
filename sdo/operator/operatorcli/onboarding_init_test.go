package operatorcli

import (
	"bytes"
	"context"
	"fmt"
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
	if err := writeOnboardingScaffoldOnly(root, entry, ""); err != nil {
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
	// The call probes run at once: init writes their workload, and the map's
	// probe lines place them in it. It sets no maxLatency: the install's soak
	// calibrates each scenario's bound.
	workload, err := os.ReadFile(filepath.Join(root, initCallWorkload))
	if err != nil || !strings.Contains(string(workload), "name: probe-call") || strings.Contains(string(workload), "scenarios") || strings.Contains(string(workload), "maxLatency") {
		t.Fatalf("call workload = %s, error = %v", workload, err)
	}
	testArch, err := os.ReadFile(filepath.Join(root, ".sdo/arch.md"))
	if err != nil || appmap.Outside(string(testArch)) != appmap.Outside(arch) {
		t.Fatalf("architecture = %s, error = %v", testArch, err)
	}
	if err := writeOnboardingScaffoldOnly(root, entry, ""); err == nil {
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
	ports := []initEntry{{service: "web", port: 80}, {service: "api", port: 8080}, {service: "db", port: 5432}}
	// With several ports, init takes the one no other component calls and
	// that holds no data, and names the others: one run, no --entry needed.
	seeded := appmap.Map{
		Components: []appmap.Component{{Name: "web", Services: []string{"web"}}, {Name: "api", Services: []string{"api"}}, {Name: "db", Services: []string{"db"}, HoldsData: true}},
		Edges:      []appmap.Edge{{From: "web", To: "api"}, {From: "api", To: "db"}},
	}
	if entry, others, err := selectInitEntry(ports, "", seeded); err != nil || entry != ports[0] || !strings.Contains(others, "api:8080") {
		t.Fatalf("selected %v, others %q, error = %v", entry, others, err)
	}
	if entry, _, err := selectInitEntry(ports, "", appmap.Map{}); err != nil || entry != ports[1] {
		t.Fatalf("without a map the first port by name: %v, %v", entry, err)
	}
	if entry, _, err := selectInitEntry(ports, "api:8080", seeded); err != nil || entry != ports[1] {
		t.Fatalf("selected %v, error = %v", entry, err)
	}
	if _, _, err := selectInitEntry(ports, "nope:1", seeded); err == nil || !strings.Contains(err.Error(), "api:8080") {
		t.Fatalf("an unknown --entry names the choices: %v", err)
	}
}

func TestOnboardingInitKeepsTheMapCompact(t *testing.T) {
	root := t.TempDir()
	const header = "# Seeded header comment\n# second line\n"
	var components strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&components, "  - {name: c%d, workload: Deployment/c%d, services: [front], holds_data: false, evidence: [manifest]}\n", i, i)
	}
	arch := "# Notes\n\n" + appmap.BeginMarker + "\n```yaml\n" + header + "version: 1\ncomponents:\n" + components.String() +
		"edges:\n  - {from: c0, to: c1, evidence: [manifest], source: \"env X\"}\noperations: []\nprobes: []\n```\n" + appmap.EndMarker + "\n\nTrailing text.\n"
	goal := "---\nschema_version: 1\njourneys:\n  - {id: browse, what: Browse items}\n---\nKeep it healthy.\n"
	for path, content := range map[string]string{".sdo/arch.md": arch, ".sdo/goal.md": goal} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeOnboardingScaffoldOnly(root, []initEntry{{service: "front", port: 80}}, ""); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(root, ".sdo/arch.md"))
	if err != nil {
		t.Fatal(err)
	}
	region, _, err := appmap.Find(string(written))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(region.YAML, header) {
		t.Fatalf("seeded header lost:\n%s", region.YAML)
	}
	if n := strings.Count(region.YAML, "name: c"); n != 20 || strings.Count(region.YAML, "\n") > 40 {
		t.Fatalf("components on %d lines, %d lines in all:\n%s", n, strings.Count(region.YAML, "\n"), region.YAML)
	}
	for _, line := range strings.Split(region.YAML, "\n") {
		if strings.HasPrefix(line, "  - ") && !strings.HasPrefix(line, "  - {") {
			t.Fatalf("list entry is not on one flow line: %q", line)
		}
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil || len(m.Components) != 20 || len(m.Edges) != 1 || m.Edges[0].Source != "env X" || len(m.Operations) != 1 || len(m.Probes) != 1 {
		t.Fatalf("parsed map = %+v, %v", m, err)
	}
	if !strings.HasPrefix(string(written), "# Notes\n\n") || !strings.HasSuffix(string(written), appmap.EndMarker+"\n\nTrailing text.\n") {
		t.Fatalf("text outside the region changed:\n%s", written)
	}
}

func writeOnboardingScaffoldOnly(root string, candidates []initEntry, requested string) error {
	_, err := writeOnboardingScaffold(root, candidates, requested)
	return err
}

// The onboarding help is a short index: each command's own --help has the
// details, so an agent reads only the page it needs.
func TestOnboardingHelpIsAShortIndex(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := RunOnboarding(context.Background(), []string{"--help"}, &out, &stderr); code != exitClean || out.Len() > 1500 {
		t.Fatalf("code %d, %d bytes:\n%s", code, out.Len(), &out)
	}
	for _, command := range []string{"init", "journeys", "check", "soak"} {
		if !strings.Contains(out.String(), "  "+command+" ") {
			t.Errorf("index lacks %s", command)
		}
	}
}
