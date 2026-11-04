package validate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// unscopedBindingDiagnostics rejects a changed incident detector that binds
// program roles by name but declares no playbook. program.BindFinding applies
// a finding's named bindings only to the playbooks it lists, and a finding
// that lists none (its detector declares none) offers them to every program:
// the detector's literal or localized target would then reach every program
// with a role of that name. Controller-generated drift findings keep that
// broad offer; a responder's incident detector must scope its claim.
func unscopedBindingDiagnostics(root string, paths []string) []Diagnostic {
	changed := map[string]bool{}
	for _, p := range paths {
		if kind, id, ok := layout.Classify(p); ok && (kind == layout.KindIncidentDetectorManifest || kind == layout.KindIncidentDetectorSource) {
			changed[id] = true
		}
	}
	ids := make([]string, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var diagnostics []Diagnostic
	for _, id := range ids {
		manifestPath := layout.DetectorManifestPath(layout.ClassIncident, id)
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(manifestPath)))
		if err != nil {
			continue
		}
		manifest, err := layout.ParseDetectorManifest(data)
		if err != nil || len(manifest.PossiblePlaybooks) > 0 {
			continue
		}
		dir := filepath.Join(root, filepath.FromSlash(layout.DetectorDir(layout.ClassIncident, id)))
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			if bindsByName(filepath.Join(dir, name)) {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleDetectorUnscopedBinding, Path: manifestPath,
					Message: "incident detector " + id + " binds program roles by name but declares no playbook, so its bindings reach every program with a role of that name",
					Hint:    "list the playbooks these bindings are for in possiblePlaybooks and Spec Playbooks; a finding's named bindings bind only the playbooks it lists"})
				break
			}
		}
	}
	return diagnostics
}

// bindsByName reports whether Go source sets finding ParameterBindings,
// directly or through localize.BindSuspects.
func bindsByName(file string) bool {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return false
	}
	found := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && (ident.Name == "ParameterBindings" || ident.Name == "BindSuspects") {
			found = true
		}
		return !found
	})
	return found
}
