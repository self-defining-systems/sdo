//go:build integration

package rendered

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/render"
)

func commitTree(t *testing.T, dir string) BaseTree {
	t.Helper()
	runGit(t, dir, "add", "-A")
	runGit(t, dir, "commit", "-q", "-m", "c")
	return BaseTree(runGit(t, dir, "rev-parse", "HEAD^{tree}"))
}

func effective(t *testing.T, store ObjectStore, base BaseTree) EffectiveTree {
	t.Helper()
	eff, err := Effective(context.Background(), store, base)
	if err != nil {
		t.Fatal(err)
	}
	return eff
}

func readAll(t *testing.T, eff EffectiveTree, name string) string {
	t.Helper()
	fsys, err := GitTreeFS(context.Background(), eff)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fsys.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// memoryRepo: an app/ tree, an override plan, memory and agent runtime files.
func memoryRepo(t *testing.T) (dir string, base BaseTree) {
	t.Helper()
	dir = t.TempDir()
	runGit(t, dir, "init", "-q")
	write(t, dir, "app/deploy.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\ndata:\n  image: a\n")
	write(t, dir, ".sdo/deployment/plan.yaml", "plan")
	write(t, dir, ".sdo/playbooks/p.md", "memory one")
	write(t, dir, ".sdo-runtime/state", "runtime")
	return dir, commitTree(t, dir)
}

func TestEffectiveIsTheCommitTreeWithoutMemory(t *testing.T) {
	dir, base := memoryRepo(t)
	eff := effective(t, ObjectStore{Dir: dir}, base)
	if eff.ID == string(base) {
		t.Fatalf("effective = %+v", eff)
	}
	if got := readAll(t, eff, "app/deploy.yaml"); !strings.Contains(got, "image: a") {
		t.Fatalf("app file = %q", got)
	}
	listing := runGitEnv(t, eff, "ls-tree", "-r", "--name-only", eff.ID)
	for _, gone := range []string{".sdo/", ".sdo-runtime/"} {
		if strings.Contains(listing, gone) {
			t.Errorf("projection still contains %s:\n%s", gone, listing)
		}
	}
}

func TestEffectiveOfATreeWithoutMemoryIsItself(t *testing.T) {
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	write(t, dir, "app/deploy.yaml", "x: 1\n")
	base := commitTree(t, dir)
	if eff := effective(t, ObjectStore{Dir: dir}, base); eff.ID != string(base) {
		t.Fatalf("effective %s of a memory-free tree %s", eff.ID, base)
	}
}

func TestEffectiveIDIsStableUnderMemoryOnlyCommitsAndMovesWithSource(t *testing.T) {
	dir, base := memoryRepo(t)
	store := ObjectStore{Dir: dir}
	first := effective(t, store, base)

	write(t, dir, ".sdo/playbooks/p.md", "memory two")
	write(t, dir, ".sdo/outcomes/x.json", "{}")
	write(t, dir, ".sdo/deployment/plan.yaml", "plan2")
	if memoryOnly := effective(t, store, commitTree(t, dir)); memoryOnly.ID != first.ID {
		t.Fatalf("memory-only commit moved the effective ID: %s -> %s", first.ID, memoryOnly.ID)
	}
	write(t, dir, "app/other.yaml", "x: 1\n")
	if moved := effective(t, store, commitTree(t, dir)); moved.ID == first.ID {
		t.Fatal("a source change must move the effective ID")
	}
}

func TestSourceReadsTheEffectiveManifests(t *testing.T) {
	dir, base := memoryRepo(t)
	commit := runGit(t, dir, "rev-parse", "HEAD")
	source := Source{Repository: dir, Deployment: rawSource(t, "app/deploy.yaml"), Catalog: testCatalog(t)}
	objs, err := source.RawManifests(context.Background(), commit, "app")
	if err != nil || len(objs) != 1 {
		t.Fatalf("RawManifests = %v, %v", objs, err)
	}
	if got, _, _ := unstructured.NestedString(objs[0].Object, "data", "image"); got != "a" {
		t.Fatalf("reads see the committed source: %v", objs[0].Object)
	}
	if _, err := fsysOf(t, effective(t, ObjectStore{Dir: dir}, base)).ReadFile(".sdo/playbooks/p.md"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("memory is not readable through the effective view: %v", err)
	}
}

func TestShapeIsStableUnderMemoryOnlyCommits(t *testing.T) {
	dir, _ := memoryRepo(t)
	shape := Source{Catalog: testCatalog(t)}.ShapeOf("app")
	rev := func() string {
		got, err := shape(context.Background(), dir, runGit(t, dir, "rev-parse", "HEAD"))
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := rev()
	write(t, dir, ".sdo/playbooks/p.md", "memory two")
	commitTree(t, dir)
	if rev() != first {
		t.Fatal("a memory-only commit changed the fallback shape")
	}
	write(t, dir, "app/second.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: d\n")
	commitTree(t, dir)
	if rev() == first {
		t.Fatal("a new resource must change the shape")
	}
}

func fsysOf(t *testing.T, eff EffectiveTree) render.FS {
	t.Helper()
	fsys, err := GitTreeFS(context.Background(), eff)
	if err != nil {
		t.Fatal(err)
	}
	return fsys
}

func runGitEnv(t *testing.T, eff EffectiveTree, args ...string) string {
	t.Helper()
	out, err := eff.Store.git(context.Background(), args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return string(out)
}
