package imagesrc_test

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

// The coordinator tells infrastructure failures from plan faults through one
// marker, InfrastructureOutcome() bool. This guards the class: every error type
// declared in imagesrc or one of its sources must implement it, so a new
// classified error cannot silently be treated as a plan fault. Wrappers that
// only attribute or unwrap a cause are exempt.
func TestEveryImageSourceErrorTypeImplementsInfrastructureOutcome(t *testing.T) {
	exempt := map[string]bool{"imagesrc.Error": true} // attribution wrapper; unwraps to the cause
	dirs := []string{".", "prebuilt", "buildkit"}
	checked := 0
	for _, dir := range dirs {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range pkgs {
			methods := map[string]map[string]bool{}
			for _, file := range pkg.Files {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
						continue
					}
					name := receiverName(fn.Recv.List[0].Type)
					if methods[name] == nil {
						methods[name] = map[string]bool{}
					}
					methods[name][fn.Name.Name] = true
				}
			}
			var names []string
			for name, set := range methods {
				if set["Error"] {
					names = append(names, name)
				}
			}
			sort.Strings(names)
			for _, name := range names {
				checked++
				if exempt[pkg.Name+"."+name] {
					continue
				}
				if !methods[name]["InfrastructureOutcome"] {
					t.Errorf("%s/%s implements error but not InfrastructureOutcome() bool: the lifecycle classifier would treat it as a plan fault", filepath.Clean(dir), name)
				}
			}
		}
	}
	if checked < 3 {
		t.Fatalf("scanned only %d error types; the scan is broken", checked)
	}
}

func receiverName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}
