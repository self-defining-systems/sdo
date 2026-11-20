//go:build integration

package bench

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// useLanesExecutable makes the platform install come from exe (the real tests' executable would run the tests again).
func useLanesExecutable(t *testing.T, exe string) {
	t.Helper()
	old := platformExecutable
	platformExecutable = func() (string, error) { return exe, nil }
	t.Cleanup(func() { platformExecutable = old })
}

// useFakeLanes stands a shell script in for the lanes binary the remote install ships.
func useFakeLanes(t *testing.T, script string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "lanes")
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	useLanesExecutable(t, p)
}

// The install puts every embedded script under <staging>/lanes-<rev>/deploy, executable, and installing again changes
// nothing (also from several callers at once).
func TestInstallPlatformScriptsIsAtomicAndIdempotent(t *testing.T) {
	staging := t.TempDir()
	var wg sync.WaitGroup
	dirs := make([]string, 4)
	for i := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := InstallPlatformScripts(staging)
			if err != nil {
				t.Error(err)
			}
			dirs[i] = d
		}()
	}
	wg.Wait()
	want, err := PlatformDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if d != want {
			t.Fatalf("installed at %q, want %q", d, want)
		}
	}
	for _, f := range []string{"lane-limits.sh", "registry.sh", "kind-lane.sh", "vcluster-lane.sh", "lane-docker-shim/docker", "host/lane-reserve"} {
		st, err := os.Stat(filepath.Join(want, "deploy", f))
		if err != nil || st.Mode()&0o111 == 0 {
			t.Errorf("deploy/%s: %v %v", f, st, err)
		}
	}
	ents, _ := os.ReadDir(want)
	if len(ents) != 1 || ents[0].Name() != "deploy" {
		t.Errorf("install left %v beside deploy", ents)
	}
	if _, err := InstallPlatformScripts(staging); err != nil {
		t.Fatal(err)
	}
}

// Staging gc never removes the platform install.
func TestGCStagingKeepsThePlatformInstall(t *testing.T) {
	staging := t.TempDir()
	dir, err := InstallPlatformScripts(staging)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(staging, "lane-a", "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	removed := GCStaging(staging, nil)
	if len(removed) != 1 || removed[0] != filepath.Join("lane-a", "old") {
		t.Fatalf("removed %v", removed)
	}
	if _, err := os.Stat(filepath.Join(dir, "deploy", "lane-limits.sh")); err != nil {
		t.Fatal(err)
	}
}

// The tar the remote install unpacks has the scripts and the step binary.
func TestPlatformTarCarriesScriptsAndBinary(t *testing.T) {
	b, err := platformTar()
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, name := range []string{"deploy/lane-limits.sh", "deploy/registry.sh", "deploy/host/lane-reserve", "bin/lanes"} {
		if !strings.Contains(s, name) {
			t.Errorf("tar lacks %s", name)
		}
	}
}

// Guard: platform code never comes from the workload checkout. No production file of this package may join a checkout
// path with deploy/ or the step binary, or run a command with the checkout as its directory; platform paths resolve
// through the lanes install (stepRunner.platform, sshHost.InstallPlatform). The workload's own files (bin/fuzzops of
// the built-in workload, an adapter's directory) are the only checkout paths.
func TestPlatformCodeNeverComesFromTheWorkloadCheckout(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	sort.Strings(files)
	fset := token.NewFileSet()
	// The one command that runs in the checkout on purpose: a host-configured cluster command (workload/operator code).
	allowDir := map[string]bool{"step.go:shell": true}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		allowed := func(pos token.Pos) bool {
			for _, d := range af.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && allowDir[f+":"+fd.Name.Name] && fd.Pos() <= pos && pos <= fd.End() {
					return true
				}
			}
			return false
		}
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				sel, ok := x.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Join" {
					return true
				}
				if f == "platform.go" || joinsPlatform(x) {
					return true
				}
				for _, a := range x.Args {
					lit, ok := a.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					if v, _ := strconv.Unquote(lit.Value); v == "deploy" || v == "fuzzops-bench" {
						t.Errorf("%s: path join with %s: platform paths come from the lanes install, not a checkout", fset.Position(x.Pos()), lit.Value)
					}
				}
			case *ast.AssignStmt:
				for i, l := range x.Lhs {
					sel, ok := l.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "Dir" || i >= len(x.Rhs) {
						continue
					}
					if rs, ok := x.Rhs[i].(*ast.SelectorExpr); ok && rs.Sel.Name == "Fuzzops" && !allowed(x.Pos()) {
						t.Errorf("%s: a command runs in the workload checkout (%s.Dir = ...Fuzzops): platform commands run in the lanes install", fset.Position(x.Pos()), "cmd")
					}
				}
			}
			return true
		})
	}
}

// joinsPlatform reports whether a Join is rooted at the lanes install: an argument is `platform` or a `.platform()` call.
func joinsPlatform(c *ast.CallExpr) bool {
	for _, a := range c.Args {
		switch x := a.(type) {
		case *ast.Ident:
			if x.Name == "platform" {
				return true
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "platform" {
				return true
			}
		}
	}
	return false
}
