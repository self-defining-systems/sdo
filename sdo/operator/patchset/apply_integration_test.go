//go:build integration

package patchset

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil/gitio"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append([]string{"-c", "user.name=t", "-c", "user.email=t@example.test", "-c", "commit.gpgsign=false", "-c", "core.autocrlf=false"}, args...)
	out, err := gitio.Run(context.Background(), dir, full...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func put(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fixture(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q")
	for n, c := range files {
		put(t, dir, n, c)
	}
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "base")
	return dir
}

func treeOf(t *testing.T, dir string) string { return git(t, dir, "rev-parse", "HEAD^{tree}") }

// patchFor edits the work tree, captures a real `git diff`, and restores it.
func patchFor(t *testing.T, dir, owner string, edit func()) string {
	t.Helper()
	return patchStep(t, dir, owner, false, edit)
}

// patchStep is patchFor; with keep the edit stays committed, so the next patch
// is authored against this one's result.
func patchStep(t *testing.T, dir, owner string, keep bool, edit func()) string {
	t.Helper()
	edit()
	git(t, dir, "add", "-A")
	diff := git(t, dir, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff") + "\n"
	if keep {
		git(t, dir, "commit", "-q", "-m", "step")
	} else {
		git(t, dir, "reset", "-q", "--hard")
		git(t, dir, "clean", "-fdq")
	}
	return "SDO-Patch: 1\nOwner: " + owner + "\nReason: |\n  " + reasonText + "\n\n" + diff
}

func series(t *testing.T, patches ...string) Series {
	t.Helper()
	var files []File
	for i, p := range patches {
		files = append(files, file(pad(i+1)+"-p.patch", p))
	}
	s, err := ParseSeries(files)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pad(n int) string { return fmt.Sprintf("%04d", n) }

func read(t *testing.T, a *Applied, path string) string {
	t.Helper()
	cmd := exec.Command("git", "cat-file", "-p", a.Tree+":"+path)
	cmd.Env = append(os.Environ(), a.Env()...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cat-file %s: %v\n%s", path, err, out)
	}
	return string(out)
}

var baseFiles = map[string]string{
	"charts/x/job.yaml": "a\nb\nc\nd\n",
	"old.txt":           "old\n",
	"README":            "readme\n",
}

func TestApplyProducesTheExpectedTreeDeterministically(t *testing.T) {
	dir := fixture(t, baseFiles)
	base := treeOf(t, dir)
	p1 := patchFor(t, dir, "deployer", func() { put(t, dir, "charts/x/job.yaml", "a\nb\nB2\nc\nd\n") })
	p2 := patchFor(t, dir, "deployer", func() {
		put(t, dir, "added/new.txt", "new\n")
		_ = os.Remove(filepath.Join(dir, "old.txt"))
	})
	s := series(t, p1, p2)
	a1, err := Apply(context.Background(), Store{Dir: dir}, base, s)
	if err != nil {
		t.Fatal(err)
	}
	defer a1.Close()
	a2, err := Apply(context.Background(), Store{Dir: dir}, base, s)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	if a1.Tree != a2.Tree {
		t.Fatalf("not deterministic: %s vs %s", a1.Tree, a2.Tree)
	}
	if got := read(t, a1, "charts/x/job.yaml"); got != "a\nb\nB2\nc\nd\n" {
		t.Fatalf("job.yaml = %q", got)
	}
	if got := read(t, a1, "added/new.txt"); got != "new\n" {
		t.Fatalf("new.txt = %q", got)
	}
	if got := read(t, a1, "README"); got != "readme\n" {
		t.Fatalf("untouched file changed: %q", got)
	}
	cmd := exec.Command("git", "cat-file", "-e", a1.Tree+":old.txt")
	cmd.Env = append(os.Environ(), a1.Env()...)
	if cmd.Run() == nil {
		t.Fatal("deleted file still present")
	}
}

func TestApplyNeverWritesTheAcceptedRepository(t *testing.T) {
	dir := fixture(t, baseFiles)
	base := treeOf(t, dir)
	p := patchFor(t, dir, "deployer", func() { put(t, dir, "charts/x/job.yaml", "a\nb\nB2\nc\nd\n") })
	before := git(t, dir, "count-objects", "-v")
	// A read-only controller mount: drop write permission everywhere.
	chmodTree(t, dir, 0o555, 0o444)
	defer chmodTree(t, dir, 0o755, 0o644)
	a, err := Apply(context.Background(), Store{Dir: dir}, base, series(t, p))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	chmodTree(t, dir, 0o755, 0o644)
	if after := git(t, dir, "count-objects", "-v"); after != before {
		t.Fatalf("object store changed:\n%s\n%s", before, after)
	}
	if out, err := exec.Command("git", "-C", dir, "cat-file", "-e", a.Tree).CombinedOutput(); err == nil {
		t.Fatalf("effective tree leaked into the accepted store: %s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, ".git", "index.lock")); err == nil {
		t.Fatal("index lock left behind")
	}
}

func chmodTree(t *testing.T, root string, dirMode, fileMode os.FileMode) {
	t.Helper()
	var dirs []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			dirs = append(dirs, p)
		} else {
			_ = os.Chmod(p, fileMode)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Chmod(dirs[i], dirMode)
	}
}

func TestPatchOrderIsMeaningful(t *testing.T) {
	dir := fixture(t, baseFiles)
	base := treeOf(t, dir)
	p1 := patchStep(t, dir, "deployer", true, func() { put(t, dir, "charts/x/job.yaml", "a\nb2\nc\nd\n") })
	p2 := patchStep(t, dir, "deployer", true, func() { put(t, dir, "charts/x/job.yaml", "a\nb3\nc\nd\n") })
	ctx := context.Background()
	good, err := Apply(ctx, Store{Dir: dir}, base, series(t, p1, p2))
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if got := read(t, good, "charts/x/job.yaml"); got != "a\nb3\nc\nd\n" {
		t.Fatalf("job.yaml = %q", got)
	}
	_, err = Apply(ctx, Store{Dir: dir}, base, series(t, p2, p1))
	if pe := mustKind(t, err, KindConflict); pe.Patch != "0001-p.patch" {
		t.Fatalf("error = %+v", pe)
	}
}

func TestApplyClassifiesRealFailures(t *testing.T) {
	dir := fixture(t, baseFiles)
	base := treeOf(t, dir)
	edit := func() { put(t, dir, "charts/x/job.yaml", "a\nb\nB2\nc\nd\n") }
	p := patchFor(t, dir, "deployer", edit)
	ctx := context.Background()

	// upstreamed: the base already carries the change.
	edit()
	git(t, dir, "commit", "-q", "-am", "upstream fix")
	_, err := Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
	mustKind(t, err, KindUpstreamed)

	// conflict: the context moved.
	put(t, dir, "charts/x/job.yaml", "a\nZ\nY\nX\nd\n")
	git(t, dir, "commit", "-q", "-am", "rewrite")
	_, err = Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
	pe := mustKind(t, err, KindConflict)
	if !strings.Contains(pe.Detail, "job.yaml") {
		t.Fatalf("detail = %q", pe.Detail)
	}

	// missing file: the target was deleted upstream.
	git(t, dir, "rm", "-q", "charts/x/job.yaml")
	git(t, dir, "commit", "-q", "-m", "delete")
	_, err = Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
	if pe := mustKind(t, err, KindMissingFile); len(pe.Paths) != 1 || pe.Paths[0] != "charts/x/job.yaml" {
		t.Fatalf("error = %+v", pe)
	}

	// the original base still applies.
	a, err := Apply(ctx, Store{Dir: dir}, base, series(t, p))
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
}

// An add over an existing file and a delete of a missing file are typed too.
func TestApplyAddOverExistingIsClassified(t *testing.T) {
	dir := fixture(t, baseFiles)
	p := patchFor(t, dir, "deployer", func() { put(t, dir, "new.txt", "one\n") })
	put(t, dir, "new.txt", "two\n")
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-q", "-m", "other new.txt")
	_, err := Apply(context.Background(), Store{Dir: dir}, treeOf(t, dir), series(t, p))
	mustKind(t, err, KindConflict)
}

// GoldenEffectiveTree pins git's result for a fixed base and series (git 2.39).
// A git behavior change that alters it must fail here, as VerifyAccepted would
// report it as a typed mismatch in production.
func TestGoldenEffectiveTree(t *testing.T) {
	dir := fixture(t, baseFiles)
	base := treeOf(t, dir)
	p1 := patchStep(t, dir, "deployer", true, func() { put(t, dir, "charts/x/job.yaml", "a\nb\nB2\nc\nd\n") })
	p2 := patchStep(t, dir, "responder", true, func() {
		put(t, dir, "added/new.txt", "new\n")
		_ = os.Remove(filepath.Join(dir, "old.txt"))
	})
	p2 = strings.Replace(p2, "Owner: responder\n", "Owner: responder\nIncident: inc-1\n", 1)
	want := treeOf(t, dir)
	a, err := Apply(context.Background(), Store{Dir: dir}, base, series(t, p1, p2))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.Tree != want {
		t.Fatalf("effective tree %s != tree of the authored commits %s", a.Tree, want)
	}
	const golden = "a6ca5347dbf11ee1fd12063f8e5f004057aaf985"
	if a.Tree != golden {
		t.Fatalf("golden tree = %s", a.Tree)
	}
}

func TestNegativeControls(t *testing.T) {
	ctx := context.Background()
	t.Run("symlink path is not followed", func(t *testing.T) {
		dir := fixture(t, map[string]string{"real/x": "x\n"})
		if err := os.Symlink("real", filepath.Join(dir, "link")); err != nil {
			t.Fatal(err)
		}
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-q", "-m", "link")
		p := "SDO-Patch: 1\nOwner: deployer\nReason: " + reasonText + "\n\ndiff --git a/link/x b/link/x\nindex 1111111111111111111111111111111111111111..2222222222222222222222222222222222222222 100644\n--- a/link/x\n+++ b/link/x\n@@ -1 +1 @@\n-x\n+y\n"
		_, err := Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
		var pe *PatchError
		if err == nil || !asPatchError(err, &pe) {
			t.Fatalf("patch through a symlink applied or failed untyped: %v", err)
		}
	})
	t.Run("attributes and config cannot change bytes", func(t *testing.T) {
		dir := fixture(t, map[string]string{".gitattributes": "* text eol=crlf\n", "f.txt": "a\nb\nc\n"})
		p := patchFor(t, dir, "deployer", func() { put(t, dir, "f.txt", "a\nb\nB\nc\n") })
		t.Setenv("GIT_CONFIG_COUNT", "1")
		t.Setenv("GIT_CONFIG_KEY_0", "core.autocrlf")
		t.Setenv("GIT_CONFIG_VALUE_0", "true")
		a, err := Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		if got := read(t, a, "f.txt"); got != "a\nb\nB\nc\n" {
			t.Fatalf("bytes changed by attributes or config: %q", got)
		}
	})
	t.Run("whitespace errors are not repaired", func(t *testing.T) {
		dir := fixture(t, map[string]string{"f.txt": "a\nb\n"})
		p := patchFor(t, dir, "deployer", func() { put(t, dir, "f.txt", "a\nb  \n\t\nc\n") })
		a, err := Apply(ctx, Store{Dir: dir}, treeOf(t, dir), series(t, p))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		if got := read(t, a, "f.txt"); got != "a\nb  \n\t\nc\n" {
			t.Fatalf("whitespace repaired: %q", got)
		}
	})
	t.Run("not a tree", func(t *testing.T) {
		dir := fixture(t, baseFiles)
		commit := git(t, dir, "rev-parse", "HEAD")
		if _, err := Apply(ctx, Store{Dir: dir}, commit, Series{}); err == nil || !strings.Contains(err.Error(), "not a tree") {
			t.Fatalf("err = %v", err)
		}
	})
}

func asPatchError(err error, target **PatchError) bool {
	for err != nil {
		if pe, ok := err.(*PatchError); ok {
			*target = pe
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
