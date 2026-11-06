package archcheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
)

// UncheckedAssertions finds single-value type assertions to a named type in
// production code: x.(Capability) on an injected collaborator panics when the
// collaborator lacks the method. Type the field as the interface it needs, or
// use the comma-ok form and return an error. Assertions to pointer, map, slice
// and basic types, and those marked "//assertcheck:ok <reason>" on the same or
// the previous line (reflection results, generic parameters), are allowed.
func UncheckedAssertions(root string) ([]string, error) {
	var findings []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == "testdata" || name == "gen" || name == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		findings = append(findings, uncheckedIn(fset, file)...)
		return nil
	})
	return findings, err
}

func uncheckedIn(fset *token.FileSet, file *ast.File) []string {
	allowed := map[int]bool{}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if strings.HasPrefix(comment.Text, "//assertcheck:ok ") {
				line := fset.Position(comment.Pos()).Line
				allowed[line], allowed[line+1] = true, true
			}
		}
	}
	checked := map[*ast.TypeAssertExpr]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			if len(s.Lhs) == 2 && len(s.Rhs) == 1 {
				markChecked(checked, s.Rhs[0])
			}
		case *ast.ValueSpec:
			if len(s.Names) == 2 && len(s.Values) == 1 {
				markChecked(checked, s.Values[0])
			}
		}
		return true
	})
	var findings []string
	ast.Inspect(file, func(n ast.Node) bool {
		assertion, ok := n.(*ast.TypeAssertExpr)
		if !ok || assertion.Type == nil || checked[assertion] || !namedType(assertion.Type) {
			return true
		}
		position := fset.Position(assertion.Pos())
		if !allowed[position.Line] {
			findings = append(findings, fmt.Sprintf("%s: unchecked assertion to %s", position, typeName(assertion.Type)))
		}
		return true
	})
	return findings
}

func markChecked(checked map[*ast.TypeAssertExpr]bool, expr ast.Expr) {
	if assertion, ok := expr.(*ast.TypeAssertExpr); ok {
		checked[assertion] = true
	}
}

func namedType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "string", "bool", "int", "int64", "uint64", "float64", "error", "any":
			return false
		}
		return true
	case *ast.SelectorExpr:
		return true
	}
	return false
}

func typeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return typeName(t.X) + "." + t.Sel.Name
	}
	return "?"
}
