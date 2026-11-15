package runtime

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Every background observer the controller runs is also run by the check's
// watch path (--evaluate-once --watch), or the watch judges frozen state and a
// break is never seen. An observer is a type in this package named *Observer
// with Start and Stop methods; evaluateOnce must construct each one.
func TestEveryObserverTheControllerRunsIsAlsoRunByTheEvaluateOnceWatch(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	methods := map[string]map[string]bool{}
	var body string
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if fn.Name.Name == "evaluateOnce" && fn.Recv == nil {
					body = sourceOf(t, fset, fn)
				}
				if fn.Recv == nil || len(fn.Recv.List) != 1 {
					continue
				}
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok && strings.HasSuffix(id.Name, "Observer") {
					if methods[id.Name] == nil {
						methods[id.Name] = map[string]bool{}
					}
					methods[id.Name][fn.Name.Name] = true
				}
			}
		}
	}
	if body == "" {
		t.Fatal("evaluateOnce not found")
	}
	if len(methods) == 0 {
		t.Fatal("no observer types found")
	}
	for name, set := range methods {
		if set["Start"] && set["Stop"] && !strings.Contains(body, name) {
			t.Errorf("%s runs in the controller but evaluateOnce never builds it: the watch would judge state that stopped updating", name)
		}
	}
}

func sourceOf(t *testing.T, fset *token.FileSet, node ast.Node) string {
	t.Helper()
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, node); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}
