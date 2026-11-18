package layout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsMemoryPathCoversTheDirectoryEntryAndEverythingBelow(t *testing.T) {
	for p, want := range map[string]bool{
		".sdo": true, ".sdo/": true, ".sdo/goal/x.md": true, ".sdo/a": true,
		".sdox": false, ".sdo.bak/x": false, "src/.sdo": false, "app.txt": false, "": false,
	} {
		if got := IsMemoryPath(p); got != want {
			t.Errorf("IsMemoryPath(%q) = %v, want %v", p, got, want)
		}
	}
}

// TestNoCallerReimplementsTheMemoryPathPredicate keeps one owner of "is this a
// memory path": the broker and lifecycle (and every other package) must ask
// layout.IsMemoryPath instead of comparing against layout.Dir themselves.
//
//ci:whole-tree
func TestNoCallerReimplementsTheMemoryPathPredicate(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains(p, "/gen/") || strings.Contains(p, "/testdata/") {
			return err
		}
		if filepath.Base(filepath.Dir(p)) == "layout" {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "HasPrefix" && len(call.Args) == 2 && mentionsLayoutDir(call.Args[1]) {
					t.Errorf("%s: strings.HasPrefix against layout.Dir; use layout.IsMemoryPath", p)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func isLayoutDir(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "layout" && sel.Sel.Name == "Dir"
}

func mentionsLayoutDir(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if x, ok := n.(ast.Expr); ok && isLayoutDir(x) {
			found = true
		}
		return !found
	})
	return found
}
