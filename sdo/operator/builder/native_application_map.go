package builder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// applicationMapFindings is the generated test's file of findings about the
// application map that need the compiled traffic scenarios.
const applicationMapFindings = "application-map-findings.json"

// generateApplicationMapTest embeds the architecture's application map in the
// generated package and adds the test that checks it against the compiled
// scenarios. The test records findings and never fails: the memory check
// decides, by actor, whether they block. Without a well-formed map nothing is
// generated; the static layer reports the map itself.
func generateApplicationMapTest(applicationRoot, workspace string) error {
	document := ""
	data, err := os.ReadFile(filepath.Join(applicationRoot, filepath.FromSlash(layout.ArchitectureFile)))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		if region, found, findErr := appmap.Find(string(data)); found && findErr == nil {
			if _, parseErr := appmap.Parse(region.YAML); parseErr == nil {
				document = region.YAML
			}
		}
	}
	// The memory host reads the map's probe-identity procedure, so the
	// constant exists, empty, without a well-formed map.
	source := "package generated\n\n// ApplicationMapYAML is the application map of the accepted architecture; empty without one.\nconst ApplicationMapYAML = " + strconv.Quote(document) + "\n"
	if document != "" {
		broken, err := knownBrokenScenarios(applicationRoot)
		if err != nil {
			return err
		}
		source += "\n// KnownBrokenScenarios are the scenarios the traffic workloads declare knownBroken.\nvar KnownBrokenScenarios = []string{"
		for _, id := range broken {
			source += strconv.Quote(id) + ", "
		}
		source += "}\n"
	}
	if err := os.WriteFile(filepath.Join(workspace, "generated", "application_map.go"), []byte(source), 0o644); err != nil {
		return err
	}
	if document == "" {
		return nil
	}
	return os.WriteFile(filepath.Join(workspace, "generated", "application_map_test.go"), []byte(applicationMapTest), 0o644)
}

// knownBrokenScenarios lists, sorted, the scenario IDs the checkout's traffic
// workloads declare knownBroken: they are not gated probes.
func knownBrokenScenarios(applicationRoot string) ([]string, error) {
	directory := filepath.Join(applicationRoot, filepath.FromSlash(layout.TrafficWorkloadsDir))
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		var decoded struct {
			Scenarios []struct {
				ID          string `yaml:"id"`
				KnownBroken any    `yaml:"knownBroken"`
			} `yaml:"scenarios"`
		}
		if yaml.Unmarshal(content, &decoded) != nil {
			continue // the yaml rule reports it
		}
		for _, scenario := range decoded.Scenarios {
			if scenario.KnownBroken != nil {
				out = append(out, scenario.ID)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

const applicationMapTest = `package generated

import (
    "encoding/json"
    "os"
    "testing"
    "sdo.dev/operator/memory/appmap"
)

// TestApplicationMapMatchesScenarios records, as findings the memory check
// classifies, where the probes' compiled scenarios disagree with the map: side
// effect against probe kind, DependsOn against the probe's path, the
// read-later setup that creates the data it reads, status-only checks, and
// probe users the map's probe-identity procedure cannot provision.
func TestApplicationMapMatchesScenarios(t *testing.T) {
    m, err := appmap.Parse(ApplicationMapYAML)
    if err != nil { t.Fatal(err) }
    scenarios := []appmap.Scenario{}
    for _, s := range TrafficCatalog() {
        users := []string{}
        for _, u := range s.Users { users = append(users, u.Name) }
        existing := []appmap.ExistingRecordsStep{}
        for _, step := range s.ExistingRecordsSteps() {
            existing = append(existing, appmap.ExistingRecordsStep{Name: step.Name, ChecksContent: step.ChecksContent, OwnKeys: step.OwnKeys})
        }
        broken := false
        for _, id := range KnownBrokenScenarios { broken = broken || id == s.ID }
        scenarios = append(scenarios, appmap.Scenario{ID: s.ID, SideEffect: string(s.SideEffect), DependsOn: s.DependsOn, SetupSteps: len(s.Setup), StatusOnly: s.StatusOnly(), Users: users, KnownBroken: broken, ExistingRecords: existing})
    }
    findings := m.CheckScenarios(scenarios)
    if findings == nil { findings = []appmap.Finding{} }
    data, err := json.Marshal(findings)
    if err != nil { t.Fatal(err) }
    if err := os.WriteFile("` + applicationMapFindings + `", data, 0o600); err != nil { t.Fatal(err) }
}
`

// ApplicationMapFindings reads the generated test's findings about the
// application map; the file is absent when the architecture has no map.
func (workspace *Workspace) ApplicationMapFindings() ([]appmap.Finding, error) {
	data, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", applicationMapFindings))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%s exceeds 1 MiB", applicationMapFindings)
	}
	var findings []appmap.Finding
	if err := json.Unmarshal(data, &findings); err != nil {
		return nil, fmt.Errorf("decode %s: %w", applicationMapFindings, err)
	}
	return findings, nil
}
