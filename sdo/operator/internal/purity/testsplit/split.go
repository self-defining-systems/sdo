// Package testsplit moves explicitly selected Go declarations to integration tests.
package testsplit

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
)

func names(decl ast.Decl) ([]string, error) {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		name := d.Name.Name
		if d.Recv != nil {
			receiver := d.Recv.List[0].Type
			if pointer, ok := receiver.(*ast.StarExpr); ok {
				receiver = pointer.X
			}
			id, ok := receiver.(*ast.Ident)
			if !ok {
				return nil, fmt.Errorf("unsupported receiver for %s", name)
			}
			name = id.Name + "." + name
		}
		return []string{name}, nil
	case *ast.GenDecl:
		if d.Tok == token.IMPORT {
			return nil, nil
		}
		result := []string{}
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				result = append(result, s.Name.Name)
			case *ast.ValueSpec:
				for _, id := range s.Names {
					result = append(result, id.Name)
				}
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("unsupported declaration")
}

// List reports declaration names; method names include their receiver.
func List(source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "input_test.go", source, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, decl := range file.Decls {
		n, err := names(decl)
		if err != nil {
			return nil, err
		}
		result = append(result, n...)
	}
	return result, nil
}

// Split preserves original declaration bytes, including embedded Go strings.
// It fails on unknown names, partial declaration groups or ambiguous imports.
// A nil unit result means every declaration moved and the source can be removed.
func Split(source []byte, selected []string) (unit, integration []byte, err error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "input_test.go", source, parser.ParseComments)
	if err != nil {
		return nil, nil, err
	}
	for _, comment := range file.Comments {
		for _, line := range comment.List {
			if strings.HasPrefix(line.Text, "//go:build") {
				return nil, nil, fmt.Errorf("source already has a build constraint")
			}
		}
	}
	wanted := map[string]bool{}
	for _, name := range selected {
		if name == "" || wanted[name] {
			return nil, nil, fmt.Errorf("empty or duplicate declaration %q", name)
		}
		wanted[name] = true
	}
	if len(wanted) == 0 {
		return nil, nil, fmt.Errorf("select at least one declaration")
	}
	seen := map[string]bool{}
	var retained, moved []ast.Decl
	for _, decl := range file.Decls {
		declared, e := names(decl)
		if e != nil {
			return nil, nil, e
		}
		if len(declared) == 0 {
			continue
		}
		matches := 0
		for _, name := range declared {
			if wanted[name] {
				seen[name] = true
				matches++
			}
		}
		if matches > 0 && matches != len(declared) {
			return nil, nil, fmt.Errorf("select the whole declaration group %v", declared)
		}
		if matches > 0 {
			moved = append(moved, decl)
		} else {
			retained = append(retained, decl)
		}
	}
	missing := []string{}
	for name := range wanted {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return nil, nil, fmt.Errorf("unknown declarations: %s", strings.Join(missing, ", "))
	}
	render := func(decls []ast.Decl, tagged bool) ([]byte, error) {
		if len(decls) == 0 {
			return nil, nil
		}
		var out bytes.Buffer
		if tagged {
			out.WriteString("//go:build integration\n\n")
		}
		out.Write(source[:fset.Position(file.Package).Offset])
		fmt.Fprintf(&out, "package %s\n\n", file.Name.Name)
		qualifiers := map[string]bool{}
		for _, decl := range decls {
			ast.Inspect(decl, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if id, ok := selector.X.(*ast.Ident); ok && id.Obj == nil {
						qualifiers[id.Name] = true
					}
				}
				return true
			})
		}
		imports := []string{}
		for _, spec := range file.Imports {
			value, e := strconv.Unquote(spec.Path.Value)
			if e != nil {
				return nil, e
			}
			alias := path.Base(value)
			if spec.Name != nil {
				alias = spec.Name.Name
			} else if !token.IsIdentifier(alias) || (strings.HasPrefix(alias, "v") && strings.Trim(alias[1:], "0123456789") == "") {
				return nil, fmt.Errorf("give import %q an explicit package alias before splitting", value)
			}
			if alias == "." {
				return nil, fmt.Errorf("dot imports cannot be split")
			}
			if alias == "_" || qualifiers[alias] {
				text := strconv.Quote(value)
				if spec.Name != nil {
					text = alias + " " + text
				}
				imports = append(imports, text)
			}
		}
		if len(imports) > 0 {
			out.WriteString("import (\n" + strings.Join(imports, "\n") + "\n)\n\n")
		}
		for _, decl := range decls {
			start := decl.Pos()
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Doc != nil {
					start = d.Doc.Pos()
				}
			case *ast.GenDecl:
				if d.Doc != nil {
					start = d.Doc.Pos()
				}
			}
			out.Write(source[fset.Position(start).Offset:fset.Position(decl.End()).Offset])
			out.WriteString("\n\n")
		}
		return format.Source(out.Bytes())
	}
	unit, err = render(retained, false)
	if err != nil {
		return nil, nil, err
	}
	integration, err = render(moved, true)
	return unit, integration, err
}
