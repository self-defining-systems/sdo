package builder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

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
	data, err := os.ReadFile(filepath.Join(applicationRoot, filepath.FromSlash(layout.ArchitectureFile)))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		return nil
	}
	if _, err := appmap.Parse(region.YAML); err != nil {
		return nil
	}
	source := "package generated\n\n// ApplicationMapYAML is the application map of the accepted architecture.\nconst ApplicationMapYAML = " + strconv.Quote(region.YAML) + "\n"
	if err := os.WriteFile(filepath.Join(workspace, "generated", "application_map.go"), []byte(source), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(workspace, "generated", "application_map_test.go"), []byte(applicationMapTest), 0o644)
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
// read-later setup that creates the data it reads, and status-only checks.
func TestApplicationMapMatchesScenarios(t *testing.T) {
    m, err := appmap.Parse(ApplicationMapYAML)
    if err != nil { t.Fatal(err) }
    scenarios := []appmap.Scenario{}
    for _, s := range TrafficCatalog() {
        scenarios = append(scenarios, appmap.Scenario{ID: s.ID, SideEffect: string(s.SideEffect), DependsOn: s.DependsOn, SetupSteps: len(s.Setup), StatusOnly: s.StatusOnly()})
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
