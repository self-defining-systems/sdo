package renderers_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/operatorcli/catalog/renderers"
)

const (
	renderDir        = "../../../render"
	architectureDocs = "../../../../../docs/architecture.md"
)

func TestCatalogRegistersBuiltIns(t *testing.T) {
	if got, want := renderers.Catalog().IDs(), []string{"helm", "kustomize", "raw"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

// TestEveryRegisteredIDHasConformanceFixtureAndDocsRow keeps the registry
// honest: a renderer cannot be added to Catalog without running the shared
// conformance suite in its own package (render/<id>) and being listed in the
// architecture component map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	docs, err := os.ReadFile(architectureDocs)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range renderers.Catalog().IDs() {
		t.Run(id, func(t *testing.T) {
			if !runsConformanceSuite(t, filepath.Join(renderDir, id), id) {
				t.Errorf("render/%s has no test calling rendertest.TestRenderer(..., %s.ID, ...)", id, id)
			}
			if !hasDocsRow(string(docs), "sdo/operator/render/"+id+"/") {
				t.Errorf("docs/architecture.md has no component-map row for `sdo/operator/render/%s/`", id)
			}
		})
	}
}

func hasDocsRow(docs, path string) bool {
	for _, line := range strings.Split(docs, "\n") {
		if strings.HasPrefix(line, "|") && strings.Contains(line, "`"+path) {
			return true
		}
	}
	return false
}

func runsConformanceSuite(t *testing.T, dir, id string) bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil || len(files) == 0 {
		return false
	}
	found := false
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isSelector(call.Fun, "rendertest", "TestRenderer") || len(call.Args) < 3 {
				return true
			}
			if isSelector(call.Args[2], id, "ID") {
				found = true
			}
			return true
		})
	}
	return found
}

func isSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	return ok && x.Name == pkg
}
