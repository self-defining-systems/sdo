package program

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// DiffTextUse is one program read of a source or baseline diff's Text outside
// evidence, such as a substring match.
type DiffTextUse struct {
	// Path is relative to the scanned memory root, with forward slashes.
	Path string
	Line int
}

// DiffTextUses finds program and shared-library code that decides on a diff's
// Text. A whole-document substring cannot tell which field or container
// changed (an added init container's image reads as app image drift), so such
// programs should compare typed fields instead. Text as a composite literal
// value (Evidence{Observed: diff.Text}) is evidence and is not reported.
func DiffTextUses(memoryRoot string) ([]DiffTextUse, error) {
	var uses []DiffTextUse
	for _, dir := range []string{"playbooks", "lib"} {
		err := filepath.WalkDir(filepath.Join(memoryRoot, dir), func(path string, entry os.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			files := token.NewFileSet()
			file, err := parser.ParseFile(files, path, nil, 0)
			if err != nil {
				return nil // syntax errors are reported by the program rules
			}
			rel, err := filepath.Rel(memoryRoot, path)
			if err != nil {
				return err
			}
			for _, position := range diffTextReads(file) {
				uses = append(uses, DiffTextUse{Path: filepath.ToSlash(rel), Line: files.Position(position).Line})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return uses, nil
}

func diffTextReads(file *ast.File) []token.Pos {
	var reads []token.Pos
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		diffs := diffVariables(function)
		var stack []ast.Node
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "Text" && isDiffValue(selector.X, diffs) && !evidenceValue(stack, selector) {
				reads = append(reads, selector.Pos())
			}
			stack = append(stack, node)
			return true
		})
	}
	return reads
}

// diffVariables names a function's parameters and variables holding a diff.
func diffVariables(function *ast.FuncDecl) map[string]bool {
	diffs := map[string]bool{}
	addTyped := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}
		for _, field := range fields.List {
			if isDiffType(field.Type) {
				for _, name := range field.Names {
					diffs[name.Name] = true
				}
			}
		}
	}
	addTyped(function.Type.Params)
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.AssignStmt:
			if len(node.Rhs) == 1 && len(node.Lhs) > 0 && isDiffCall(node.Rhs[0]) {
				if name, ok := node.Lhs[0].(*ast.Ident); ok {
					diffs[name.Name] = true
				}
			}
		case *ast.ValueSpec:
			if isDiffType(node.Type) || (len(node.Values) == 1 && isDiffCall(node.Values[0])) {
				for _, name := range node.Names {
					diffs[name.Name] = true
				}
			}
		}
		return true
	})
	return diffs
}

func isDiffType(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	return ok && (selector.Sel.Name == "Diff" || selector.Sel.Name == "SourceDiff")
}

func isDiffCall(expression ast.Expr) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && (selector.Sel.Name == "DiffAgainstSource" || selector.Sel.Name == "DiffAgainstBaseline")
}

func isDiffValue(expression ast.Expr, diffs map[string]bool) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && diffs[identifier.Name]
}

func evidenceValue(stack []ast.Node, selector *ast.SelectorExpr) bool {
	if len(stack) == 0 {
		return false
	}
	pair, ok := stack[len(stack)-1].(*ast.KeyValueExpr)
	return ok && pair.Value == selector
}
