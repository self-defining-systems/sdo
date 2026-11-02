package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The memory host edge reaches the evidence sink of a program's read view by
// walking UnderlyingReadView. A wrapper that embeds a ReadView without exposing
// it hides the sink, and the host's reads (and its failures) are lost.
func TestReadViewWrappersExposeTheViewTheyWrap(t *testing.T) {
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	wrappers, exposed := map[string]bool{}, map[string]bool{}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				switch decl := decl.(type) {
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						typeSpec, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						if structType, ok := typeSpec.Type.(*ast.StructType); ok && embedsReadView(structType) {
							wrappers[typeSpec.Name.Name] = true
						}
					}
				case *ast.FuncDecl:
					if decl.Recv != nil && decl.Name.Name == "UnderlyingReadView" {
						exposed[receiverName(decl.Recv.List[0].Type)] = true
					}
				}
			}
		}
	}
	for name := range wrappers {
		if !exposed[name] {
			t.Errorf("%s embeds a ReadView but has no UnderlyingReadView method", name)
		}
	}
	if len(wrappers) == 0 {
		t.Fatal("found no read view wrappers; the check is not looking at the package")
	}
}

func embedsReadView(structType *ast.StructType) bool {
	for _, field := range structType.Fields.List {
		if len(field.Names) != 0 {
			continue
		}
		if selector, ok := field.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "ReadView" {
			return true
		}
	}
	return false
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
