package builder

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestMemoryValidationAppliesEveryControllerStartCheck keeps accepted memory
// loadable: every validation the controller applies to its detectors at start
// must also run in the generated workspace test that memory check and the
// validator Job execute. A new startup check fails here until it is added.
func TestMemoryValidationAppliesEveryControllerStartCheck(t *testing.T) {
	startup := map[string][]string{
		"../runtime/controller.go": {"NewController"},
		"../core/run.go":           {"RunWithOptions"},
		"../core/runner.go":        {"RunOnce"},
	}
	checks := map[string]bool{}
	for path, functions := range startup {
		for _, check := range validationCalls(t, path, functions) {
			checks[check] = true
		}
	}
	if !checks["core.ValidateDetectors"] {
		t.Fatalf("controller startup checks = %v; expected core.ValidateDetectors among them", checks)
	}
	missing := []string{}
	for check := range checks {
		if !strings.Contains(controllerStartTest, check+"(All())") {
			missing = append(missing, check)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("controller startup checks missing from memory validation: %v", missing)
	}
}

// takesDetectorSet reports whether a call receives the controller's detector
// set, as opposed to per-finding checks made while it runs.
func takesDetectorSet(call *ast.CallExpr) bool {
	for _, argument := range call.Args {
		switch value := argument.(type) {
		case *ast.Ident:
			if value.Name == "detectors" {
				return true
			}
		case *ast.SelectorExpr:
			if value.Sel.Name == "Detectors" {
				return true
			}
		}
	}
	return false
}

// validationCalls returns the Validate* calls on the detector set,
// package-qualified as core.X, made in the named functions of one source file.
func validationCalls(t *testing.T, path string, functions []string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[string]bool{}
	for _, name := range functions {
		wanted[name] = true
	}
	found := map[string]bool{}
	calls := []string{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !wanted[function.Name.Name] {
			continue
		}
		found[function.Name.Name] = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !takesDetectorSet(call) {
				return true
			}
			switch callee := call.Fun.(type) {
			case *ast.Ident:
				if strings.HasPrefix(callee.Name, "Validate") && filepath.Base(filepath.Dir(path)) == "core" {
					calls = append(calls, "core."+callee.Name)
				}
			case *ast.SelectorExpr:
				if pkg, ok := callee.X.(*ast.Ident); ok && pkg.Name == "core" && strings.HasPrefix(callee.Sel.Name, "Validate") {
					calls = append(calls, "core."+callee.Sel.Name)
				}
			}
			return true
		})
	}
	for name := range wanted {
		if !found[name] {
			t.Fatalf("%s no longer defines startup function %s; update this test", path, name)
		}
	}
	return calls
}
