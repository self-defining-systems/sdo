package testsplit

import (
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

func needsIntegration(expr constraint.Expr) bool {
	switch e := expr.(type) {
	case *constraint.TagExpr:
		return e.Tag == "integration"
	case *constraint.AndExpr:
		return needsIntegration(e.X) || needsIntegration(e.Y)
	case *constraint.OrExpr:
		return needsIntegration(e.X) && needsIntegration(e.Y)
	}
	return false
}

// ProcessClosure reports declarations depending on a real subprocess or on an
// integration-only declaration. It is migration guidance, not an admission gate:
// review candidates and verify both build selections before writing the split.
func ProcessClosure(sources map[string][]byte) (map[string][]string, error) {
	type node struct {
		file   string
		names  []string
		decl   ast.Decl
		tagged bool
		direct bool
	}
	nodes := []node{}
	process := map[string]bool{}
	paths := make([]string, 0, len(sources))
	for name := range sources {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	mark := func(name string) {
		if name == "_" {
			return
		}
		process[name] = true
		if receiver, _, ok := strings.Cut(name, "."); ok {
			process[receiver] = true
		}
	}
	for _, name := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), name, sources[name], parser.ParseComments)
		if err != nil {
			return nil, err
		}
		tagged := false
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if strings.HasPrefix(comment.Text, "//go:build ") {
					expr, err := constraint.Parse(comment.Text)
					if err != nil {
						return nil, err
					}
					tagged = needsIntegration(expr)
				}
			}
		}
		executors := map[string]bool{}
		for _, spec := range file.Imports {
			value, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			if value == "os/exec" {
				alias := "exec"
				if spec.Name != nil {
					alias = spec.Name.Name
				}
				executors[alias] = true
			}
		}
		for _, decl := range file.Decls {
			declared, err := names(decl)
			if err != nil {
				return nil, err
			}
			if len(declared) == 0 {
				continue
			}
			direct := tagged
			ast.Inspect(decl, func(n ast.Node) bool {
				if selector, ok := n.(*ast.SelectorExpr); ok {
					if id, ok := selector.X.(*ast.Ident); ok && id.Obj == nil && executors[id.Name] {
						direct = true
					}
				}
				return true
			})
			if direct {
				for _, n := range declared {
					mark(n)
				}
			}
			nodes = append(nodes, node{name, declared, decl, tagged, direct})
		}
	}
	changed := true
	for changed {
		changed = false
		for _, n := range nodes {
			dependent := false
			ast.Inspect(n.decl, func(value ast.Node) bool {
				if id, ok := value.(*ast.Ident); ok && process[id.Name] {
					dependent = true
				}
				return true
			})
			if dependent {
				for _, name := range n.names {
					if !process[name] {
						mark(name)
						changed = true
					}
				}
			}
		}
	}
	result := map[string][]string{}
	for _, n := range nodes {
		if n.tagged {
			continue
		}
		move := n.direct
		for _, name := range n.names {
			move = move || process[name]
		}
		if move {
			result[n.file] = append(result[n.file], n.names...)
		}
	}
	for name := range result {
		sort.Strings(result[name])
	}
	return result, nil
}
