package safefs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The guard is structural: it walks every non-test Go file under sdo/operator,
// so a package that starts writing files is covered the day it appears. Three
// checks, each against a reviewed table in guard_owned_test.go:
//  1. direct file writers in os and io/ioutil, resolved through import
//     aliases and dot imports;
//  2. every file that can start a subprocess (os/exec), because a subprocess
//     writes with its own path-based syscalls, which safefs cannot confine;
//  3. subprocess arguments naming a writer verb (git apply, checkout, clean,
//     cp, tar, sh -c, ...).
// A listed file that stops matching must be deleted from its table.

// writerCalls are the functions that create, replace, move, re-mode or delete
// files. Each follows symlinks (or creates through them) and so must not run
// on a path below an agent-controlled checkout.
var writerCalls = map[string]bool{
	"WriteFile": true, "Create": true, "OpenFile": true, "Rename": true, "MkdirAll": true,
	"Mkdir": true, "Remove": true, "RemoveAll": true, "Symlink": true, "Link": true,
	"Chmod": true, "Truncate": true, "CreateTemp": true, "MkdirTemp": true,
	"Chown": true, "Lchown": true, "Chtimes": true, "TempFile": true, "TempDir": true,
}

// writerImports are the import paths whose writerCalls are checked.
var writerImports = map[string]bool{"os": true, "io/ioutil": true}

// writerVerbs are literals that turn a subprocess into a tree writer when it
// is aimed at a checkout.
var writerVerbs = map[string]bool{
	"apply": true, "am": true, "patch": true, "checkout": true, "restore": true, "clean": true,
	"reset": true, "cp": true, "mv": true, "rm": true, "tar": true, "unzip": true,
	"sh": true, "bash": true, "install": true, "rsync": true,
}

// findings are what one source file does that the guard cares about.
type findings struct {
	writers []string // e.g. "os.WriteFile"
	execs   bool     // calls into os/exec
	verbs   []string // writer verbs among string literals, only when execs
}

// scan reports the guarded behavior of one Go source file.
func scan(t *testing.T, filename string, src any) findings {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	names := map[string]string{} // local identifier -> import path
	var f findings
	for _, spec := range file.Imports {
		importPath, _ := strconv.Unquote(spec.Path.Value)
		local := path.Base(importPath)
		if spec.Name != nil {
			local = spec.Name.Name
		}
		if local == "." && (writerImports[importPath] || importPath == "os/exec") {
			f.writers = append(f.writers, "dot import of "+importPath)
			continue
		}
		names[local] = importPath
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		switch importPath := names[pkg.Name]; {
		case writerImports[importPath] && writerCalls[sel.Sel.Name]:
			f.writers = append(f.writers, importPath+"."+sel.Sel.Name)
		case importPath == "os/exec" && strings.HasPrefix(sel.Sel.Name, "Command"):
			f.execs = true
		}
		return true
	})
	if f.execs {
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil && writerVerbs[s] {
					f.verbs = append(f.verbs, s)
				}
			}
			return true
		})
	}
	return f
}

// productionFiles lists every non-test Go file under sdo/operator, relative to
// it, skipping testdata trees.
func productionFiles(t *testing.T) []string {
	t.Helper()
	root := ".."
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			rel, _ := filepath.Rel(root, p)
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	return files
}

//ci:whole-tree
func TestCheckoutWritersGoThroughSafefs(t *testing.T) {
	files := productionFiles(t)
	if len(files) < 100 {
		t.Fatalf("walk found only %d production files; the guard is not seeing the module", len(files))
	}
	var offenders []string
	seenWriter, seenExec, seenVerb := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, rel := range files {
		f := scan(t, filepath.Join("..", filepath.FromSlash(rel)), nil)
		if len(f.writers) > 0 {
			seenWriter[rel] = true
			if operatorOwned[rel] == "" {
				offenders = append(offenders, rel+": "+strings.Join(f.writers, ", "))
			}
		}
		if f.execs {
			seenExec[rel] = true
			if subprocessOwned[rel] == "" {
				offenders = append(offenders, rel+": starts a subprocess (os/exec)")
			}
		}
		if len(f.verbs) > 0 {
			seenVerb[rel] = true
			if verbOwned[rel] == "" {
				offenders = append(offenders, rel+": subprocess writer verbs "+strings.Join(f.verbs, ", "))
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("file writers or subprocesses outside safefs; use safefs, or list the file in the owning table in guard_owned_test.go with a reason it never sees agent-controlled paths:\n  %s", strings.Join(offenders, "\n  "))
	}
	for table, seen := range map[string]struct {
		owned map[string]string
		seen  map[string]bool
	}{"operatorOwned": {operatorOwned, seenWriter}, "subprocessOwned": {subprocessOwned, seenExec}, "verbOwned": {verbOwned, seenVerb}} {
		for rel := range seen.owned {
			if !seen.seen[rel] {
				t.Errorf("%s: %s no longer matches; delete its entry", table, rel)
			}
		}
	}
}

// TestScannerSeesEvasions pins what the scanner recognizes, so the guard
// cannot silently narrow: aliased and dot imports, ioutil, the wider os names,
// exec through an alias, and writer verbs.
func TestScannerSeesEvasions(t *testing.T) {
	cases := []struct {
		name, src string
		writers   int
		execs     bool
		verbs     []string
	}{
		{"plain", `package p; import "os"; func f() { os.WriteFile("a", nil, 0) }`, 1, false, nil},
		{"aliased os", `package p; import o "os"; func f() { o.Chtimes("a", t, t) }`, 1, false, nil},
		{"ioutil", `package p; import "io/ioutil"; func f() { ioutil.WriteFile("a", nil, 0) }`, 1, false, nil},
		{"chown", `package p; import "os"; func f() { os.Lchown("a", 1, 1) }`, 1, false, nil},
		{"dot import", `package p; import . "os"; func f() { WriteFile("a", nil, 0) }`, 1, false, nil},
		{"exec alias", `package p; import x "os/exec"; func f() { x.CommandContext(nil, "git", "apply", "p.diff") }`, 0, true, []string{"apply"}},
		{"exec shell", `package p; import "os/exec"; func f() { exec.Command("sh", "-c", "cp a b") }`, 0, true, []string{"sh"}},
		{"reader only", `package p; import "os"; func f() { os.ReadFile("a"); os.Stat("b") }`, 0, false, nil},
		{"verb without exec", `package p; var v = "apply"`, 0, false, nil},
	}
	for _, c := range cases {
		got := scan(t, c.name+".go", c.src)
		if len(got.writers) != c.writers || got.execs != c.execs || strings.Join(got.verbs, ",") != strings.Join(c.verbs, ",") {
			t.Errorf("%s: got %+v, want writers=%d execs=%v verbs=%v", c.name, got, c.writers, c.execs, c.verbs)
		}
	}
}
