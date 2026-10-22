// Package purity checks application-owned dependency graphs of declared pure
// cores. Generated protobuf/runtime support is trusted, not re-linted.
package purity

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

func inspectSyntax(fset *token.FileSet, file *ast.File) []string {
	var failures []string
	report := func(node ast.Node, reason string) {
		failures = append(failures, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), reason))
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		imported, _ := strconv.Unquote(spec.Path.Value)
		name := path.Base(imported)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = imported
		if name == "." {
			report(spec, "pure core forbids dot imports")
		}
		if forbiddenImport(imported) {
			report(spec, "pure core imports edge dependency "+imported)
		}
	}
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
			report(gen, "pure core forbids mutable package variables")
		}
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "init" {
			report(fn, "pure core forbids init functions")
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.GoStmt:
			report(n, "pure core forbids goroutines")
		case *ast.ChanType:
			report(n, "pure core forbids channel types")
		case *ast.SelectorExpr:
			if owner, ok := n.X.(*ast.Ident); ok && imports[owner.Name] == "time" {
				switch n.Sel.Name {
				case "Now", "Since", "Until", "Sleep", "After", "AfterFunc", "Tick", "NewTimer", "NewTicker":
					report(n, "pure core requires time on events, not time."+n.Sel.Name)
				}
			}
		}
		return true
	})
	sort.Strings(failures)
	return failures
}

func forbiddenImport(imported string) bool {
	for _, prefix := range []string{"k8s.io/client-go", "net", "os", "math/rand", "crypto/rand", "io", "context"} {
		if imported == prefix || strings.HasPrefix(imported, prefix+"/") {
			return true
		}
	}
	return strings.HasPrefix(imported, "sdo.dev/operator/") && strings.Contains(imported, "/internal/")
}

// Check loads production packages only and checks every application-owned
// transitive dependency. Standard library and third-party runtime internals are
// outside the architecture boundary (for example protobuf itself uses globals).
func Check(directory string, pattern string) []string {
	loaded, err := packages.Load(&packages.Config{Dir: directory, Mode: packages.NeedName | packages.NeedFiles | packages.NeedImports | packages.NeedDeps}, pattern)
	if err != nil {
		return []string{err.Error()}
	}
	var failures []string
	seen := map[string]bool{}
	roots := 0
	var visit func(*packages.Package)
	visit = func(pkg *packages.Package) {
		if seen[pkg.PkgPath] {
			return
		}
		seen[pkg.PkgPath] = true
		for _, err := range pkg.Errors {
			failures = append(failures, err.Error())
		}
		if !strings.HasPrefix(pkg.PkgPath, "sdo.dev/") || strings.Contains(pkg.PkgPath, "/contracts/gen/") {
			return
		}
		for _, filename := range pkg.GoFiles {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filename, nil, 0)
			if err != nil {
				failures = append(failures, err.Error())
				continue
			}
			failures = append(failures, inspectSyntax(fset, file)...)
		}
		for _, dependency := range pkg.Imports {
			visit(dependency)
		}
	}
	for _, pkg := range loaded {
		roots++
		if len(pkg.GoFiles) == 0 && len(pkg.Errors) == 0 {
			failures = append(failures, "pure core has no production files: "+pkg.PkgPath)
		}
		visit(pkg)
	}
	if roots == 0 {
		failures = append(failures, "pure core pattern matched no packages: "+pattern)
	}
	sort.Strings(failures)
	return failures
}
