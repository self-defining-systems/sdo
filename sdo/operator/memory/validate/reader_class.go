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

// readerClassDiagnostics rejects a changed incident-class detector package
// that implements appstate.Reader. A reader exposes application state to the
// controller on its own schedule; like health detectors it belongs to the
// detection designer and the human upkeep import, and responders, whose
// reflection grant covers incident detectors only, must not add one. The
// existing ownership grants then keep readers away from responders, so this
// is the only new rule.
//
// Limit: a name match in the detector's own non-test files, like
// unscopedBindingDiagnostics: a ReadState method, or a reference to
// appstate.Reader. A package that only reads a reader's state through
// appstate.Require is an ordinary detector.
func readerClassDiagnostics(root string, paths []string) []Diagnostic {
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
			if implementsReader(filepath.Join(dir, name)) {
				path := layout.DetectorDir(layout.ClassIncident, id) + "/" + name
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleDetectorReaderClass, Path: path,
					Message: "incident detector " + id + " implements appstate.Reader, and a reader must be a health detector",
					Hint:    "app-state readers belong to the detection designer: put the reader in .sdo/diagnostics/detectors/health/<id>/; an incident detector reads a reader's state with appstate.Require"})
				break
			}
		}
	}
	return diagnostics
}

// implementsReader reports whether Go source declares a ReadState method or
// names appstate.Reader.
func implementsReader(file string) bool {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return false
	}
	found := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			found = found || n.Recv != nil && n.Name.Name == "ReadState"
		case *ast.SelectorExpr:
			if pkg, ok := n.X.(*ast.Ident); ok && pkg.Name == "appstate" && n.Sel.Name == "Reader" {
				found = true
			}
		}
		return !found
	})
	return found
}
