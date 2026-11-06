//go:build integration

package rendered

import (
	"context"
	"errors"
	"io/fs"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
)

const reasonText = "The vendored manifest must differ so the app starts against the local registry."

// patchText builds a real patch by editing the work tree, as a capture tool
// would, then restoring it.
func patchText(t *testing.T, dir string, edit func()) string {
	t.Helper()
	edit()
	runGit(t, dir, "add", "-A")
	diff := runGit(t, dir, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff") + "\n"
	runGit(t, dir, "reset", "-q", "--hard")
	runGit(t, dir, "clean", "-fdq")
	return "SDO-Patch: 1\nOwner: deployer\nReason: |\n  " + reasonText + "\n\n" + diff
}

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
	t.Cleanup(eff.Close)
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

// patchedRepo: an upstream-shaped app/ tree, memory, a plan, values, and one
// patch that edits app/deploy.yaml.
func patchedRepo(t *testing.T) (dir string, base BaseTree) {
	t.Helper()
	dir = t.TempDir()
	runGit(t, dir, "init", "-q")
	write(t, dir, "app/deploy.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\ndata:\n  image: a\n")
	write(t, dir, ".sdo/deployment/values/v.yaml", "v: 1\n")
	write(t, dir, ".sdo/deployment/plan.yaml", "plan")
	write(t, dir, ".sdo/playbooks/p.md", "memory one")
	runGit(t, dir, "add", "-A")
	runGit(t, dir, "commit", "-q", "-m", "base")
	p := patchText(t, dir, func() {
		write(t, dir, "app/deploy.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\ndata:\n  image: patched\n")
	})
	write(t, dir, patchset.Directory+"/0001-image.patch", p)
	return dir, commitTree(t, dir)
}

func TestEffectiveAppliesPatchesAndProjectsTheDeploymentClass(t *testing.T) {
	dir, base := patchedRepo(t)
	eff := effective(t, ObjectStore{Dir: dir}, base)
	if eff.ID == string(base) || eff.Patches != 1 || eff.PatchesDigest == "" || eff.PatchesDigest == emptyDigest {
		t.Fatalf("effective = %+v", eff)
	}
	if got := readAll(t, eff, "app/deploy.yaml"); got != "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\ndata:\n  image: patched\n" {
		t.Fatalf("patched file = %q", got)
	}
	if got := readAll(t, eff, ".sdo/deployment/values/v.yaml"); got != "v: 1\n" {
		t.Fatalf("values must stay: %q", got)
	}
	// The projected tree itself (not just the filtered view) has no memory, plan or patches.
	listing := runGitEnv(t, eff, "ls-tree", "-r", "--name-only", eff.ID)
	for _, gone := range []string{".sdo/playbooks", ".sdo/deployment/plan.yaml", patchset.Directory} {
		if strings.Contains(listing, gone) {
			t.Errorf("projection still contains %s:\n%s", gone, listing)
		}
	}
	// The accepted repository is untouched: it still holds the unpatched file and no new tree.
	if out := runGit(t, dir, "show", string(base)+":app/deploy.yaml"); !strings.Contains(out, "image: a") {
		t.Fatalf("accepted tree was altered: %q", out)
	}
	if exec := runGit(t, dir, "cat-file", "-t", string(base)); exec != "tree" {
		t.Fatal(exec)
	}
	if err := gitErr(dir, "cat-file", "-e", eff.ID); err == nil {
		t.Fatal("effective objects leaked into the accepted object store")
	}
}

func TestEffectiveIDIsStableUnderMemoryOnlyCommitsAndMovesWithSourceOrPatches(t *testing.T) {
	dir, base := patchedRepo(t)
	store := ObjectStore{Dir: dir}
	first := effective(t, store, base)

	write(t, dir, ".sdo/playbooks/p.md", "memory two")
	write(t, dir, ".sdo/outcomes/x.json", "{}")
	write(t, dir, ".sdo/deployment/plan.yaml", "plan2")
	memoryOnly := effective(t, store, commitTree(t, dir))
	if memoryOnly.ID != first.ID {
		t.Fatalf("memory-only commit moved the effective ID: %s -> %s", first.ID, memoryOnly.ID)
	}

	write(t, dir, ".sdo/deployment/values/v.yaml", "v: 2\n")
	if moved := effective(t, store, commitTree(t, dir)); moved.ID == first.ID {
		t.Fatal("a values change must move the effective ID")
	}
	write(t, dir, "app/other.yaml", "x: 1\n")
	if moved := effective(t, store, commitTree(t, dir)); moved.ID == first.ID {
		t.Fatal("a source change must move the effective ID")
	}
	runGit(t, dir, "rm", "-q", patchset.Directory+"/0001-image.patch")
	noPatch := effective(t, store, commitTree(t, dir))
	if got := readAll(t, noPatch, "app/deploy.yaml"); !strings.Contains(got, "image: a") || noPatch.Patches != 0 {
		t.Fatalf("without the patch the file is upstream's: %q (%+v)", got, noPatch)
	}
}

func TestEffectiveMemoizesAcrossCallsWithOneComposition(t *testing.T) {
	dir, base := patchedRepo(t)
	var compositions atomic.Int32
	store := ObjectStore{Dir: dir, Compose: func(ctx context.Context, d, b string, s patchset.Series, drop []string) (Composed, error) {
		compositions.Add(1)
		return composeGit(ctx, d, b, s, drop)
	}}
	a := effective(t, store, base)
	b := effective(t, store, base)
	if a.ID != b.ID || compositions.Load() != 1 {
		t.Fatalf("IDs %s %s, compositions %d", a.ID, b.ID, compositions.Load())
	}
}

func TestEffectiveReportsATypedConflict(t *testing.T) {
	dir, _ := patchedRepo(t)
	// Upstream moves the patched line: the patch no longer applies.
	write(t, dir, "app/deploy.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\ndata:\n  image: upstream-changed\n")
	base := commitTree(t, dir)
	_, err := Effective(context.Background(), ObjectStore{Dir: dir}, base)
	var pe *patchset.PatchError
	if !errors.As(err, &pe) || pe.Kind != patchset.KindConflict || pe.Patch != "0001-image.patch" || !errors.Is(err, ErrSourceInvalid) {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceReadsTheEffectiveManifests(t *testing.T) {
	dir, base := patchedRepo(t)
	commit := runGit(t, dir, "rev-parse", "HEAD")
	source := Source{Repository: dir, Deployment: rawSource(t, "app/deploy.yaml"), Catalog: testCatalog(t)}
	objs, err := source.RawManifests(context.Background(), commit, "app")
	if err != nil || len(objs) != 1 {
		t.Fatalf("RawManifests = %v, %v", objs, err)
	}
	if got, _, _ := unstructured.NestedString(objs[0].Object, "data", "image"); got != "patched" {
		t.Fatalf("reads must see the patched source: %v", objs[0].Object)
	}
	if _, err := fsysOf(t, effective(t, ObjectStore{Dir: dir}, base)).ReadFile(".sdo/playbooks/p.md"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("memory is not readable through the effective view: %v", err)
	}
}

func TestShapeOfWithoutADeploymentSourceIsTheEffectiveID(t *testing.T) {
	dir, _ := patchedRepo(t)
	shape := Source{}.ShapeOf("app")
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
	runGit(t, dir, "rm", "-q", patchset.Directory+"/0001-image.patch")
	commitTree(t, dir)
	if rev() == first {
		t.Fatal("dropping a patch must change the fallback shape")
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

func gitErr(dir string, args ...string) error {
	return exec.Command("git", append([]string{"-C", dir}, args...)...).Run()
}
