package upstreams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/operatorcli/catalog/upstreams"
)

const (
	upstreamDir      = "../../../upstream"
	architectureDocs = "../../../../../docs/architecture.md"
)

// implementation maps a watcher ID to its package under upstream/. Unlike
// renderers, the ID ("git", the kind in sources.yaml) names what is followed
// while the package names how (gitpoll).
var implementation = map[string]string{"git": "gitpoll"}

func TestCatalogRegistersBuiltIns(t *testing.T) {
	if got, want := upstreams.Catalog().IDs(), []string{"git"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

// TestEveryRegisteredIDHasConformanceFixtureAndDocsRow keeps the registry
// honest: a watcher cannot be added to Catalog without running the shared
// conformance suite in its own package and being listed in the architecture
// component map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	docs, err := os.ReadFile(architectureDocs)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range upstreams.Catalog().IDs() {
		t.Run(id, func(t *testing.T) {
			pkg, ok := implementation[id]
			if !ok {
				t.Fatalf("watcher %q has no entry in the implementation table of this test", id)
			}
			if !runsConformanceSuite(t, filepath.Join(upstreamDir, pkg), pkg) {
				t.Errorf("upstream/%s has no test calling upstreamtest.TestWatcher(..., %s.ID, ...)", pkg, pkg)
			}
			if !hasDocsRow(string(docs), "sdo/operator/upstream/"+pkg+"/") {
				t.Errorf("docs/architecture.md has no component-map row for `sdo/operator/upstream/%s/`", pkg)
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

func runsConformanceSuite(t *testing.T, dir, pkg string) bool {
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
			if !ok || !isSelector(call.Fun, "upstreamtest", "TestWatcher") || len(call.Args) < 3 {
				return true
			}
			// Inside the package itself the ID is the bare identifier.
			if id, ok := call.Args[2].(*ast.Ident); ok && id.Name == "ID" && parsed.Name.Name == pkg {
				found = true
			}
			if isSelector(call.Args[2], pkg, "ID") {
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
