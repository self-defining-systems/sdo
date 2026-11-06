package images_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/operatorcli/catalog/images"
)

const (
	imagesrcDir      = "../../../imagesrc"
	architectureDocs = "../../../../../docs/architecture.md"
)

func testConfig() images.Config {
	return images.Config{Builder: buildkit.Builder{AllowedRegistryHost: "reg.example"}, BuildBudget: time.Minute}
}

func TestCatalogRegistersBuiltIns(t *testing.T) {
	c, err := images.Catalog(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.IDs(), []string{"buildkit", "prebuilt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
	if c.Default() != "prebuilt" {
		t.Fatalf("default source = %q, want prebuilt", c.Default())
	}
}

func TestCatalogRejectsInvalidConfig(t *testing.T) {
	for name, mutate := range map[string]func(*images.Config){
		"no push host":    func(c *images.Config) { c.Builder.AllowedRegistryHost = "" },
		"path as host":    func(c *images.Config) { c.Builder.AllowedRegistryHost = "reg.example/ns" },
		"negative budget": func(c *images.Config) { c.BuildBudget = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			mutate(&cfg)
			if _, err := images.Catalog(cfg); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// TestEveryRegisteredIDHasConformanceFixtureAndDocsRow keeps the registry
// honest: a source cannot be added to Catalog without running the shared
// catalog contract in its own package (imagesrc/<id>) and being listed in the
// architecture component map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	docs, err := os.ReadFile(architectureDocs)
	if err != nil {
		t.Fatal(err)
	}
	c, err := images.Catalog(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs() {
		t.Run(id, func(t *testing.T) {
			if !runsCatalogContract(t, filepath.Join(imagesrcDir, id)) {
				t.Errorf("imagesrc/%s has no test calling imagesrctest.TestCatalogContract(..., ID)", id)
			}
			if !hasDocsRow(string(docs), "sdo/operator/imagesrc/"+id+"/") {
				t.Errorf("docs/architecture.md has no component-map row for `sdo/operator/imagesrc/%s/`", id)
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

// runsCatalogContract reports whether a test in dir calls
// imagesrctest.TestCatalogContract with the package's ID constant.
func runsCatalogContract(t *testing.T, dir string) bool {
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
			if !ok || len(call.Args) < 3 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "TestCatalogContract" {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "imagesrctest" {
				if id, ok := call.Args[2].(*ast.Ident); ok && id.Name == "ID" {
					found = true
				}
			}
			return true
		})
	}
	return found
}
