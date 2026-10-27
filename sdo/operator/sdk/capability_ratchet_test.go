package sdk_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Optional detector capabilities (observation consumers, windows,
// confirmation) are found with sdk.As so a decorator cannot hide them. A direct
// type assertion on a detector, or a decorator that embeds a Detector without
// Unwrap, brings back the bug where a scaled run never attached the pod
// admission and reachability observations.
func TestDetectorCapabilitiesAreReadThroughAs(t *testing.T) {
	root := filepath.Join("..")
	fset := token.NewFileSet()
	unwrap := map[string]bool{}
	embeds := map[string]token.Position{}
	var assertions []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		if strings.Contains(path, "testdata") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if n.Name.Name == "As" && n.Recv == nil && file.Name.Name == "sdk" {
					return false
				}
				if n.Name.Name == "Unwrap" && n.Recv != nil && len(n.Recv.List) == 1 {
					unwrap[receiverName(n.Recv.List[0].Type)] = true
				}
			case *ast.TypeAssertExpr:
				if n.Type != nil && isDetectorOperand(n.X) {
					assertions = append(assertions, fset.Position(n.Pos()).String())
				}
			case *ast.TypeSpec:
				if structType, ok := n.Type.(*ast.StructType); ok {
					for _, field := range structType.Fields.List {
						if len(field.Names) == 0 && isDetectorType(field.Type) {
							embeds[n.Name.Name] = fset.Position(n.Pos())
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
	for _, position := range assertions {
		t.Errorf("%s: read detector capabilities with sdk.As, not a type assertion", position)
	}
	for name, position := range embeds {
		if !unwrap[name] {
			t.Errorf("%s: %s embeds a Detector and must implement Unwrap() sdk.Detector", position, name)
		}
	}
}

func isDetectorOperand(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name == "detector"
	case *ast.SelectorExpr:
		return x.Sel.Name == "Detector"
	}
	return false
}

func isDetectorType(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name == "Detector"
	case *ast.SelectorExpr:
		return x.Sel.Name == "Detector"
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
