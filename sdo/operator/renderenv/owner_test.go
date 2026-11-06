package renderenv_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The render environment has one owner. Everything that renders (deploy,
// desired-state reads, restart verification, upstream advances) obtains its
// render.Env from renderenv.For over the recorded environment; no other
// production package builds one with install facts. Renderers (render/...) only
// read an Env, and test files may build whatever they exercise.
func TestOnlyTheOwnerConstructsARenderEnv(t *testing.T) {
	root := ".."
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if rel == "render" || rel == "renderenv" || strings.HasPrefix(rel, "render/") || strings.Contains(rel, "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CompositeLit:
				if selector, ok := n.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "Env" && isIdent(selector.X, "render") && len(n.Elts) > 0 {
					violations = append(violations, fset.Position(n.Pos()).String()+": render.Env literal with fields")
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if selector, ok := lhs.(*ast.SelectorExpr); ok {
						switch selector.Sel.Name {
						case "KubeVersion", "APIVersions", "Release":
							violations = append(violations, fset.Position(n.Pos()).String()+": assigns render environment field "+selector.Sel.Name)
						}
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
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("render.Env is built only by package renderenv (renderenv.For); fix:\n%s", strings.Join(violations, "\n"))
	}
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}
