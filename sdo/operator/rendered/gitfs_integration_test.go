//go:build integration

package rendered

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"sdo.dev/operator/render"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.test", "-c", "commit.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func write(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixtureRepo commits a synthetic tree with a symlink, a gitlink, memory under
// .sdo/ and a patch directory.
func fixtureRepo(t *testing.T) (dir string, tree BaseTree) {
	t.Helper()
	dir = t.TempDir()
	runGit(t, dir, "init", "-q")
	write(t, dir, "app/deploy.yaml", "kind: Deployment\n")
	write(t, dir, "app/sub/svc.yaml", "kind: Service\n")
	write(t, dir, "top.txt", "top")
	write(t, dir, ".sdo/goal.md", "memory")
	write(t, dir, ".sdo/deployment/values/patch.yaml", "patch")
	if err := os.Symlink("top.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, "top.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", "-A")
	runGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+strings.Repeat("a", 40)+",vendor/mod")
	runGit(t, dir, "commit", "-q", "-m", "fixture")
	return dir, BaseTree(runGit(t, dir, "rev-parse", "HEAD^{tree}"))
}

func openTree(t *testing.T, objects ObjectStore, base BaseTree) (render.FS, EffectiveTree) {
	t.Helper()
	eff, err := Effective(context.Background(), objects, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(eff.Close)
	fsys, err := GitTreeFS(context.Background(), eff)
	if err != nil {
		t.Fatal(err)
	}
	return fsys, eff
}

func TestGitTreeFSExposesOnlyTrackedRegularFilesAndDeploymentMemory(t *testing.T) {
	dir, base := fixtureRepo(t)
	fsys, eff := openTree(t, ObjectStore{Dir: dir}, base)
	if eff.ID == string(base) {
		t.Fatalf("the effective tree projects .sdo memory away, so it is not the base tree %s", base)
	}
	var names []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			names = append(names, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	want := ".sdo/deployment/values/patch.yaml app/deploy.yaml app/sub/svc.yaml top.txt"
	if got := strings.Join(names, " "); got != want {
		t.Fatalf("files = %q, want %q (symlink, gitlink and .sdo memory must be absent)", got, want)
	}
	for _, absent := range []string{"link", "vendor/mod", "vendor", ".sdo/goal.md"} {
		if _, err := fsys.Stat(absent); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat(%s) = %v, want not exist", absent, err)
		}
		if _, err := fsys.ReadFile(absent); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("ReadFile(%s) = %v, want not exist", absent, err)
		}
	}
	root, _ := fsys.ReadDir(".")
	var top []string
	for _, e := range root {
		top = append(top, e.Name())
	}
	if got := strings.Join(top, " "); got != ".sdo app top.txt" {
		t.Fatalf("root entries = %q", got)
	}
	if err := fstest.TestFS(fsys, "app/deploy.yaml", "app/sub/svc.yaml", "top.txt", ".sdo/deployment/values/patch.yaml"); err != nil {
		t.Fatal(err)
	}
}

func TestGitTreeFSBlobAndSizeComeFromTheListingWithoutReadingBytes(t *testing.T) {
	dir, base := fixtureRepo(t)
	var catFiles atomic.Int32
	objects := ObjectStore{Dir: dir, Git: func(ctx context.Context, d string, args ...string) ([]byte, error) {
		if args[0] == "cat-file" && args[1] == "blob" {
			catFiles.Add(1)
		}
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", d}, args...)...)
		cmd.Env = append(os.Environ(), GitEnvFrom(ctx)...)
		return cmd.Output()
	}}
	fsys, _ := openTree(t, objects, base)
	blob, err := fsys.Blob("app/deploy.yaml")
	if err != nil || blob != render.GitBlobID([]byte("kind: Deployment\n")) {
		t.Fatalf("Blob = %q, %v", blob, err)
	}
	info, err := fsys.Stat("app/deploy.yaml")
	if err != nil || info.Size() != int64(len("kind: Deployment\n")) || info.IsDir() {
		t.Fatalf("Stat = %v, %v", info, err)
	}
	if info, _ := fsys.Stat("top.txt"); info.Mode().Perm() != 0o755 {
		t.Fatalf("executable mode lost: %v", info.Mode())
	}
	if d, err := fsys.Stat("app"); err != nil || !d.IsDir() {
		t.Fatalf("Stat(app) = %v, %v", d, err)
	}
	if _, err := fsys.Blob("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Blob(missing) = %v", err)
	}
	if n := catFiles.Load(); n != 0 {
		t.Fatalf("Blob/Stat read %d blobs", n)
	}
	data, err := fsys.ReadFile("app/deploy.yaml")
	if err != nil || string(data) != "kind: Deployment\n" || catFiles.Load() != 1 {
		t.Fatalf("ReadFile = %q, %v, reads %d", data, err, catFiles.Load())
	}
	if _, err := fsys.ReadFile("app"); err == nil {
		t.Fatal("reading a directory must fail")
	}
}

func TestGitTreeFSRejectsInvalidPaths(t *testing.T) {
	dir, base := fixtureRepo(t)
	fsys, _ := openTree(t, ObjectStore{Dir: dir}, base)
	for _, name := range []string{"../top.txt", "/top.txt", "app//deploy.yaml", ""} {
		if _, err := fsys.ReadFile(name); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("ReadFile(%q) = %v, want invalid", name, err)
		}
	}
}

func TestProposalTreeIsReadFromTheCheckoutObjectStore(t *testing.T) {
	accepted, base := fixtureRepo(t)
	checkout := filepath.Join(t.TempDir(), "checkout")
	runGit(t, accepted, "clone", "-q", "--no-local", accepted, checkout)
	write(t, checkout, "app/new.yaml", "kind: ConfigMap\n")
	runGit(t, checkout, "add", "-A")
	runGit(t, checkout, "commit", "-q", "-m", "proposal")
	proposal := BaseTree(runGit(t, checkout, "rev-parse", "HEAD^{tree}"))
	if proposal == base {
		t.Fatal("fixture proposal must differ")
	}

	ctx := context.Background()
	if _, err := Effective(ctx, ObjectStore{Dir: accepted}, proposal); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("proposal tree in the accepted root = %v, want ErrSourceUnavailable", err)
	}
	fsys, _ := openTree(t, ObjectStore{Dir: checkout}, proposal)
	data, err := fsys.ReadFile("app/new.yaml")
	if err != nil || string(data) != "kind: ConfigMap\n" {
		t.Fatalf("proposal read = %q, %v", data, err)
	}
	// The same accepted commit reads identical blobs from either store.
	a, _ := openTree(t, ObjectStore{Dir: accepted}, base)
	c, _ := openTree(t, ObjectStore{Dir: checkout}, base)
	ba, _ := a.Blob("app/deploy.yaml")
	bc, _ := c.Blob("app/deploy.yaml")
	if ba != bc {
		t.Fatalf("blob differs between stores: %s vs %s", ba, bc)
	}
}

func TestEffectiveAndGitTreeFSRejectNonTrees(t *testing.T) {
	dir, _ := fixtureRepo(t)
	commit := runGit(t, dir, "rev-parse", "HEAD")
	ctx := context.Background()
	if _, err := Effective(ctx, ObjectStore{Dir: dir}, BaseTree(commit)); err == nil || errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("a commit is not a tree: %v", err)
	}
	for _, id := range []string{"HEAD", "HEAD^{tree}", "", "abc"} {
		if _, err := Effective(ctx, ObjectStore{Dir: dir}, BaseTree(id)); err == nil {
			t.Errorf("Effective(%q) must fail", id)
		}
		if _, err := GitTreeFS(ctx, EffectiveTree{ID: id, Store: ObjectStore{Dir: dir}}); err == nil {
			t.Errorf("GitTreeFS(%q) must fail", id)
		}
	}
	if _, err := GitTreeFS(ctx, EffectiveTree{ID: commit, Store: ObjectStore{Dir: dir}}); err == nil {
		t.Fatal("GitTreeFS must not list a commit")
	}
}

func TestGitTreeFSReadFailureMidRenderIsUnavailable(t *testing.T) {
	dir, base := fixtureRepo(t)
	fail := false
	objects := ObjectStore{Dir: dir, Git: func(ctx context.Context, d string, args ...string) ([]byte, error) {
		if fail && args[0] == "cat-file" {
			return nil, errors.New("disk gone")
		}
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", d}, args...)...)
		cmd.Env = append(os.Environ(), GitEnvFrom(ctx)...)
		return cmd.Output()
	}}
	fsys, _ := openTree(t, objects, base)
	fail = true
	if _, err := fsys.ReadFile("top.txt"); !errors.Is(err, ErrSourceUnavailable) || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadFile = %v", err)
	}
}
