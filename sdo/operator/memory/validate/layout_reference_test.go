package validate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// The examples are classified by the real layout and authorized by the real
// grants table. The AST inventory below prevents a new kind going undocumented.
func renderLayoutReference(t *testing.T) string {
	t.Helper()
	paths := []string{
		layout.SchemaVersionFile, layout.GoalFile, layout.ArchitectureFile,
		layout.LifecycleProvenanceFile, layout.DiagnosticsGoModFile, layout.DiagnosticsGoSumFile,
		layout.LegacyDetectorManifestFile,
		layout.DetectorManifestPath(layout.ClassHealth, "detector-id"),
		layout.DetectorDir(layout.ClassHealth, "detector-id") + "/detector.go",
		layout.DetectorManifestPath(layout.ClassIncident, "detector-id"),
		layout.DetectorDir(layout.ClassIncident, "detector-id") + "/detector.go",
		layout.TrafficGeneratorsDir + "/application/generator.go",
		layout.TrafficGeneratorsDir + "/incident/generator.go",
		layout.TrafficWorkloadsDir + "/application.yaml", layout.TrafficWorkloadsDir + "/incident-example.yaml",
		layout.PlaybookIndexFile, layout.PlaybookDir("playbook-id") + "/README.md",
		layout.PlaybookDir(layout.HealthObjectivePlaybook) + "/README.md",
		layout.PlaybookDir("playbook-id") + "/diagnose.go", layout.PlaybookDir("playbook-id") + "/mitigate.go",
		layout.PlaybookDir("playbook-id") + "/program_test.go",
		layout.PlaybookScriptPath("playbook-id", "repair.sh"),
		layout.LibrariesDir + "/library-id/helper.go", layout.LibrariesDir + "/library-id/metadata.yaml",
		layout.CorpusDir + "/case-id/open.json", layout.CorpusDir + "/case-id/fixed.json",
		layout.CorpusDir + "/case-id/healthy.json", layout.CorpusDir + "/case-id/recordings.json",
		layout.CorpusDir + "/case-id/labels.json", layout.CorpusDir + "/case-id/operator.json",
		layout.CorpusDir + "/case-id/accepted.json", layout.RenderedLockFile,
		layout.RenderedDir + "/" + strings.Repeat("0", 64) + ".yaml",
		layout.OutcomePath("incident-id"), layout.LegacyOutcomesFile,
	}
	var out strings.Builder
	out.WriteString("# Operational-memory layout\n\nGENERATED from `memory/layout/paths.go` and `memory/validate/ownership.go`. Do not edit.\nRegenerate from `sdo/operator` with `SDO_UPDATE_LAYOUT_REFERENCE=1 go test ./memory/validate -run '^TestMemoryLayoutReference$'`.\n\n")
	fmt.Fprintf(&out, "Schema version: %d. Paths are repository-relative and used only while editing a proposal checkout; worker results and memory prose use logical IDs.\n\n", layout.CurrentSchemaVersion)
	fmt.Fprintf(&out, "- Playbook, library and corpus IDs: `%s`.\n- Detector IDs: `%s`.\n- Script IDs: `<playbook-id>/<filename>.sh`.\n- Outcomes: SHA-256 of incident ID; rendered objects: 64 lowercase hexadecimal characters.\n\n", layout.PlaybookIDPattern, layout.DetectorIDPattern)
	out.WriteString("## Paths, kinds and write authority\n\n| Path | Kind | Logical ID | Writer and phase |\n| --- | --- | --- | --- |\n")
	seen := map[string]bool{}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		if !ok {
			t.Fatalf("reference path no longer classifies: %s", p)
		}
		seen[string(kind)] = true
		authority := owners(kind, id)
		display := p
		for from, to := range map[string]string{"playbook-id": "<playbook-id>", "detector-id": "<detector-id>", "library-id": "<library-id>", "case-id": "<case-id>", strings.Repeat("0", 64): "<object-hash>"} {
			display = strings.ReplaceAll(display, from, to)
		}
		if kind == layout.KindOutcome {
			display = layout.OutcomesDir + "/<sha256(incident-id)>.json"
			id = "<sha256(incident-id)>"
		}
		if kind == layout.KindRenderedObject {
			id = "<object-hash>"
		}
		if id == "" {
			id = "—"
		}
		fmt.Fprintf(&out, "| `%s` | `%s` | `%s` | %s |\n", display, kind, id, authority)
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "layout", "paths.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if !strings.HasPrefix(name.Name, "Kind") || i >= len(spec.Values) {
				continue
			}
			lit, ok := spec.Values[i].(*ast.BasicLit)
			if !ok {
				continue
			}
			v, err := strconv.Unquote(lit.Value)
			if err == nil && !seen[v] {
				t.Errorf("layout kind %s needs a reference path", name.Name)
			}
		}
		return true
	})
	return out.String() + "\nThe controller also writes bootstrap schema/module metadata and immutable corpus captures through trusted storage edges; these are not agent grants. The import phase admits human-supplied learned memory; upkeep migrates existing formats. No agent writes outcomes, rendered objects or lifecycle provenance. Source files outside this layout belong to the deployer in the initial phase or responder in the proposal phase.\n\nDetector packages may contain additional source/test files; traffic generators may contain package files. Program files are `diagnose.go`, optional `mitigate.go` and `*_test.go`. Incident traffic is the `incident` generator and `incident-*` workloads; all other traffic belongs to the health judge. Legacy manifest and outcome paths exist for migration only. Preserve accepted provenance and corpus cases.\n"
}

func TestMemoryLayoutReference(t *testing.T) {
	expected := renderLayoutReference(t)
	target := filepath.Join(skillsRoot, "sdo-memory", "references", "layout.md")
	if os.Getenv("SDO_UPDATE_LAYOUT_REFERENCE") == "1" {
		if err := os.WriteFile(target, []byte(expected), 0644); err != nil {
			t.Fatal(err)
		}
	}
	actual, err := os.ReadFile(target)
	if err != nil || string(actual) != expected {
		t.Fatalf("layout.md is stale: regenerate with SDO_UPDATE_LAYOUT_REFERENCE=1 go test ./memory/validate -run '^TestMemoryLayoutReference$' (%v)", err)
	}
}
