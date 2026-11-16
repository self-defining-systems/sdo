package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// A byte limit sent with a pod-log request makes Kubernetes return the OLDEST
// bytes of the window, so busy containers show stale lines and never the
// fault. Log requests ask for the newest lines (TailLines) and bound bytes
// client-side (program.NewestLogBytes); this fails if any non-test code sets
// LimitBytes on a request to anything but nil.
//
//ci:whole-tree
func TestNoPodLogRequestSetsLimitBytes(t *testing.T) {
	root := ".."
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "/gen/") || strings.Contains(path, "/testdata/") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		report := func(pos token.Pos, value ast.Expr) {
			if ident, ok := value.(*ast.Ident); ok && ident.Name == "nil" {
				return
			}
			t.Errorf("%s: LimitBytes on a pod-log request returns the oldest bytes; use TailLines and program.NewestLogBytes", fset.Position(pos))
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.KeyValueExpr:
				if key, ok := n.Key.(*ast.Ident); ok && key.Name == "LimitBytes" {
					report(n.Pos(), n.Value)
				}
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "LimitBytes" && len(n.Lhs) == len(n.Rhs) {
						report(n.Pos(), n.Rhs[i])
					}
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
