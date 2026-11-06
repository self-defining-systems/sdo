// Package catalogtest is the one completeness convention for plugin catalogs
// (renderers, image sources, upstream watchers). Each catalog's test names an
// explicit ID to package map and calls Completeness; nothing is inferred from
// a name, so an ID that differs from its package ("git" is implemented by
// gitpoll) is handled the same way as one that matches.
package catalogtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Spec describes one catalog family.
type Spec struct {
	// IDs are the IDs the catalog registers.
	IDs []string
	// Packages maps every registered ID to its implementation package, the
	// directory name under FamilyDir. It is the explicit registry: each ID
	// needs an entry, each entry an ID, and each implementation directory an
	// entry.
	Packages map[string]string
	// FamilyDir is the family directory (for example "../../../render"),
	// relative to the calling test's package.
	FamilyDir string
	// DocsPath is the family's path in the architecture map's component rows
	// (for example "sdo/operator/render/").
	DocsPath string
	// DocsFile is the architecture document, relative to the calling test.
	DocsFile string
	// SupportDirs are family children that hold no implementation
	// ("rendertest"); "internal" is always skipped.
	SupportDirs []string
	// Suite names the shared conformance function every implementation's
	// tests must call as pkg.Func(t, ..., ID, ...) with the ID constant as its
	// third argument, for example "rendertest.TestRenderer".
	Suite string
}

// Completeness fails the test for every problem Problems finds.
func Completeness(t *testing.T, spec Spec) {
	t.Helper()
	problems, err := Problems(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range problems {
		t.Error(problem)
	}
}

// Problems lists where the registry, the implementation packages, their
// conformance suite runs and the architecture docs rows do not line up.
func Problems(spec Spec) ([]string, error) {
	suitePkg, suiteFunc, ok := strings.Cut(spec.Suite, ".")
	if !ok {
		return nil, fmt.Errorf("catalogtest: Suite %q is not pkg.Func", spec.Suite)
	}
	docs, err := os.ReadFile(spec.DocsFile)
	if err != nil {
		return nil, err
	}
	var problems []string
	registered := map[string]bool{}
	for _, id := range spec.IDs {
		registered[id] = true
	}
	for id := range spec.Packages {
		if !registered[id] {
			problems = append(problems, fmt.Sprintf("implementation table names %q, which the catalog does not register; delete the entry", id))
		}
	}
	mapped := map[string]bool{}
	for _, id := range spec.IDs {
		pkg, ok := spec.Packages[id]
		if !ok {
			problems = append(problems, fmt.Sprintf("catalog registers %q but this test's implementation table has no entry for it", id))
			continue
		}
		mapped[pkg] = true
		dir := filepath.Join(spec.FamilyDir, pkg)
		called, err := callsSuite(dir, pkg, suitePkg, suiteFunc)
		if err != nil {
			return nil, err
		}
		if !called {
			problems = append(problems, fmt.Sprintf("%s has no test calling %s(..., ID, ...) with the package's ID constant", dir, spec.Suite))
		}
		if !hasDocsRow(string(docs), spec.DocsPath+pkg+"/") {
			problems = append(problems, fmt.Sprintf("docs/architecture.md has no component-map row for `%s%s/`", spec.DocsPath, pkg))
		}
	}
	entries, err := os.ReadDir(spec.FamilyDir)
	if err != nil {
		return nil, err
	}
	skip := map[string]bool{"internal": true}
	for _, d := range spec.SupportDirs {
		skip[d] = true
	}
	for _, entry := range entries {
		if entry.IsDir() && !skip[entry.Name()] && !mapped[entry.Name()] {
			problems = append(problems, fmt.Sprintf("%s/%s is an implementation package that no registered ID maps to; register it or move it", spec.FamilyDir, entry.Name()))
		}
	}
	sort.Strings(problems)
	return problems, nil
}

func hasDocsRow(docs, path string) bool {
	for _, line := range strings.Split(docs, "\n") {
		if strings.HasPrefix(line, "|") && strings.Contains(line, "`"+path) {
			return true
		}
	}
	return false
}

// callsSuite reports whether a test in dir calls suitePkg.suiteFunc with the
// package's ID constant (bare ID inside the package, pkg.ID elsewhere) as the
// third argument.
func callsSuite(dir, pkg, suitePkg, suiteFunc string) (bool, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		return false, err
	}
	found := false
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			return false, err
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 3 || !isSelector(call.Fun, suitePkg, suiteFunc) {
				return true
			}
			if id, ok := call.Args[2].(*ast.Ident); ok && id.Name == "ID" && strings.TrimSuffix(parsed.Name.Name, "_test") == pkg {
				found = true
			}
			if isSelector(call.Args[2], pkg, "ID") {
				found = true
			}
			return true
		})
	}
	return found, nil
}

func isSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	return ok && x.Name == pkg
}
