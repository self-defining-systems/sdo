package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A server polls its requests directory, so a test that writes a request there
// in place lets the server read half a file (flaky "unexpected end of JSON
// input", #665 and #709). Tests must publish through PublishRequest.
//
// The check is syntactic: a write call (os.WriteFile, os.Create, os.OpenFile)
// whose path mentions the literal "requests", directly or through an identifier
// assigned from such an expression in the same function, is flagged unless a
// string literal in the path starts with "." (a dot-prefixed temporary name,
// which the server skips).
func nonAtomicRequestWrites(file *ast.File) []token.Pos {
	var found []token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		var body *ast.BlockStmt
		switch fn := node.(type) {
		case *ast.FuncDecl:
			body = fn.Body
		case *ast.FuncLit:
			body = fn.Body
		}
		if body == nil {
			return true
		}
		polled := map[string]bool{}
		ast.Inspect(body, func(n ast.Node) bool {
			if _, nested := n.(*ast.FuncLit); nested {
				return false
			}
			switch assign := n.(type) {
			case *ast.AssignStmt:
				if len(assign.Lhs) == len(assign.Rhs) {
					for i, lhs := range assign.Lhs {
						if name, ok := lhs.(*ast.Ident); ok {
							polled[name.Name] = mentionsRequests(assign.Rhs[i], polled)
						}
					}
				}
			case *ast.CallExpr:
				if writesFile(assign) && len(assign.Args) > 0 && mentionsRequests(assign.Args[0], polled) {
					found = append(found, assign.Pos())
				}
			}
			return true
		})
		return true
	})
	return found
}

func writesFile(call *ast.CallExpr) bool {
	fn, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := fn.X.(*ast.Ident)
	if !ok || pkg.Name != "os" {
		return false
	}
	switch fn.Sel.Name {
	case "WriteFile", "Create", "OpenFile":
		return true
	}
	return false
}

// mentionsRequests reports whether a path expression names a requests
// directory without a hidden (dot-prefixed) literal component.
func mentionsRequests(expr ast.Expr, polled map[string]bool) bool {
	requests, hidden := false, false
	ast.Inspect(expr, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BasicLit:
			if v.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(v.Value)
			if err != nil {
				return true
			}
			requests = requests || text == "requests"
			hidden = hidden || strings.HasPrefix(text, ".")
		case *ast.Ident:
			requests = requests || polled[v.Name]
		}
		return true
	})
	return requests && !hidden
}

func TestRequestDirWritePolicyNegativeControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ os.WriteFile(filepath.Join(dir, "requests", name), payload, 0o600) }`,
		`package p; func f(){ path := filepath.Join(root, "requests", "call.json"); os.WriteFile(path, payload, 0o600) }`,
		`package p; func f(){ p := filepath.Join(root, "requests", "a.json"); q := p; os.Create(q) }`,
		`package p; func f(){ go func(){ os.WriteFile(filepath.Join(d, "requests", n), b, 0o600) }() }`,
	} {
		if len(nonAtomicRequestWrites(parseSource(t, source))) != 1 {
			t.Errorf("missed an in-place request write: %s", source)
		}
	}
	for _, source := range []string{
		`package p; func f(){ os.WriteFile(filepath.Join(dir, "requests", "."+name), payload, 0o600) }`,
		`package p; func f(){ tmp := filepath.Join(dir, "requests", ".call.json"); os.WriteFile(tmp, payload, 0o600) }`,
		`package p; func f(){ testutil.PublishRequest(t, filepath.Join(dir, "requests"), name, payload) }`,
		`package p; func f(){ os.WriteFile(filepath.Join(dir, "responses", name), payload, 0o600) }`,
		`package p; func f(){ n := filepath.Join(dir, "requests", "a.json"); n = filepath.Join(dir, "other"); os.WriteFile(n, b, 0o600) }`,
	} {
		if got := nonAtomicRequestWrites(parseSource(t, source)); len(got) != 0 {
			t.Errorf("rejected a safe write: %s", source)
		}
	}
}

func TestTestsPublishRequestsAtomically(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && path != root && (entry.Name() == "third_party" || entry.Name() == "testdata" || entry.Name() == "node_modules" || strings.HasPrefix(entry.Name(), ".")) {
			return fs.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, nil, 0)
		if err != nil {
			return err
		}
		for _, pos := range nonAtomicRequestWrites(file) {
			t.Errorf("%s: write into a polled requests directory; use testutil.PublishRequest (write a dot-prefixed temporary, then rename)", positions.Position(pos))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
