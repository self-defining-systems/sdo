package render_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Renderer implementations (sub-packages of render other than rendertest) are
// pure functions of (FS, Env, config): they may not touch the network, the
// process, the environment, or the clock. Until the archcheck rule for this
// lands, this test enforces it for every implementation package.

var bannedImports = map[string]bool{
	"net": true, "net/http": true, "os": true, "os/exec": true, "os/signal": true,
	"syscall": true, "math/rand": true, "math/rand/v2": true, "crypto/rand": true,
}

func purityViolations(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (d.Name() == "rendertest" || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || filepath.Dir(path) == root {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		names := map[string]string{}
		for _, spec := range file.Imports {
			imported, _ := strconv.Unquote(spec.Path.Value)
			if bannedImports[imported] {
				out = append(out, fset.Position(spec.Pos()).String()+": renderer imports "+imported)
			}
			name := filepath.Base(imported)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			names[name] = imported
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && names[id.Name] == "time" {
				switch sel.Sel.Name {
				case "Now", "Since", "Until", "Sleep", "After", "AfterFunc", "Tick", "NewTimer", "NewTicker":
					out = append(out, fset.Position(sel.Pos()).String()+": renderer reads the clock via time."+sel.Sel.Name)
				}
			}
			return true
		})
		return nil
	})
	return out, err
}

func TestRendererImplementationsArePure(t *testing.T) {
	got, err := purityViolations(".")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > 0 {
		t.Fatalf("renderer implementations must be pure:\n%s", strings.Join(got, "\n"))
	}
}

func TestPurityCheckCatchesViolations(t *testing.T) {
	dir := t.TempDir()
	impl := filepath.Join(dir, "bad")
	if err := os.Mkdir(impl, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "package bad\n\nimport (\n\t\"os\"\n\t\"net/http\"\n\t\"time\"\n)\n\nvar _ = os.Getenv\nvar _ = http.Get\nvar _ = time.Now\n"
	if err := os.WriteFile(filepath.Join(impl, "bad.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := purityViolations(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 violations (os, net/http, time.Now), got %v", got)
	}
}
