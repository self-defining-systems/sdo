package rendered

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"sdo.dev/operator/patchset"
)

const unitPatch = `SDO-Patch: 1
Owner: deployer
Reason: |
  The vendored deployment cannot pull from the local registry without a policy.

diff --git a/app/a.txt b/app/a.txt
index 2e65efe2a145dda7ee51d1741299f848e5bf752e..63d8dbd40c23542e740659a7168a0ce3138ea748 100644
--- a/app/a.txt
+++ b/app/a.txt
@@ -1 +1 @@
-a
+b
`

// fakeRepo answers the listings and blob reads Effective makes, from maps of
// directory -> synthetic ls-tree records and blob -> bytes.
type fakeRepo struct {
	dirs  map[string][]string
	blobs map[string]string
	cat   atomic.Int32
}

func (f *fakeRepo) git(_ context.Context, _ string, args ...string) ([]byte, error) {
	switch {
	case args[0] == "cat-file" && args[1] == "-t":
		return []byte("tree\n"), nil
	case args[0] == "cat-file" && args[1] == "blob":
		f.cat.Add(1)
		if b, ok := f.blobs[args[2]]; ok {
			return []byte(b), nil
		}
	case args[0] == "ls-tree":
		dir := strings.TrimSuffix(args[len(args)-1], "/")
		return []byte(strings.Join(f.dirs[dir], "\x00") + "\x00"), nil
	}
	return nil, fmt.Errorf("fake repo: unexpected git %v", args)
}

func rec(kind, name string) string {
	mode := "100644"
	if kind == "tree" {
		mode = "040000"
	}
	return fmt.Sprintf("%s %s %s\t%s", mode, kind, strings.Repeat("e", 40), name)
}

// composeRecorder is a ComposeFunc that records its inputs and counts releases.
type composeRecorder struct {
	calls, closed atomic.Int32
	drop          []string
	series        patchset.Series
	err           error
}

func (c *composeRecorder) compose(_ context.Context, _, base string, series patchset.Series, drop []string) (Composed, error) {
	c.calls.Add(1)
	c.drop, c.series = drop, series
	if c.err != nil {
		return Composed{}, c.err
	}
	return Composed{Tree: strings.Repeat("f", 40), Env: []string{"GIT_X=1"}, Close: func() { c.closed.Add(1) }}, nil
}

func tid(c byte) BaseTree { return BaseTree(strings.Repeat(string(c), 40)) }

func memoryRepo() *fakeRepo {
	return &fakeRepo{
		dirs: map[string][]string{
			".sdo":             {rec("tree", ".sdo/deployment"), rec("tree", ".sdo/playbooks"), rec("tree", ".sdo/outcomes")},
			".sdo/deployment":  {rec("tree", ".sdo/deployment/patches"), rec("blob", ".sdo/deployment/plan.yaml"), rec("tree", ".sdo/deployment/values"), rec("tree", ".sdo/deployment/overlays"), rec("tree", ".sdo/deployment/scratch")},
			patchset.Directory: {strings.Replace(rec("blob", patchset.Directory+"/0001-fix.patch"), strings.Repeat("e", 40), strings.Repeat("1", 40), 1)},
		},
		blobs: map[string]string{strings.Repeat("1", 40): unitPatch},
	}
}

func TestEffectiveDropsEverythingButDeploymentViewsAndAppliesTheSeries(t *testing.T) {
	repo, rc := memoryRepo(), &composeRecorder{}
	store := ObjectStore{Dir: t.Name(), Git: repo.git, Compose: rc.compose}
	eff, err := Effective(context.Background(), store, tid('a'))
	if err != nil {
		t.Fatal(err)
	}
	defer eff.Close()
	want := ".sdo/playbooks .sdo/outcomes .sdo/deployment/patches .sdo/deployment/plan.yaml .sdo/deployment/scratch"
	if got := strings.Join(rc.drop, " "); got != want {
		t.Fatalf("dropped = %q, want %q (values and overlays stay, patches/plan/unknown dirs go)", got, want)
	}
	if rc.series.Len() != 1 || eff.Patches != 1 || eff.PatchesDigest != rc.series.Digest() || eff.PatchesDigest == emptyDigest {
		t.Fatalf("series = %d, effective = %+v", rc.series.Len(), eff)
	}
	if eff.ID != strings.Repeat("f", 40) || len(eff.Store.Env) != 1 || eff.Store.Dir != store.Dir {
		t.Fatalf("effective tree must read through the composed private store: %+v", eff)
	}
}

func TestEffectiveIsTheBaseTreeWhenNothingChanges(t *testing.T) {
	repo := &fakeRepo{dirs: map[string][]string{
		".sdo":            {rec("tree", ".sdo/deployment")},
		".sdo/deployment": {rec("tree", ".sdo/deployment/values"), rec("tree", ".sdo/deployment/build")},
	}}
	rc := &composeRecorder{}
	eff, err := Effective(context.Background(), ObjectStore{Dir: t.Name(), Git: repo.git, Compose: rc.compose}, tid('b'))
	if err != nil {
		t.Fatal(err)
	}
	defer eff.Close()
	if eff.ID != string(tid('b')) || rc.calls.Load() != 0 || eff.Patches != 0 || eff.PatchesDigest != emptyDigest {
		t.Fatalf("a tree that is its own projection needs no write: %+v, compose calls %d", eff, rc.calls.Load())
	}
	empty := &fakeRepo{dirs: map[string][]string{}}
	eff2, err := Effective(context.Background(), ObjectStore{Dir: t.Name() + "2", Git: empty.git, Compose: rc.compose}, tid('c'))
	if err != nil || eff2.ID != string(tid('c')) {
		t.Fatalf("no .sdo at all: %+v %v", eff2, err)
	}
}

func TestEffectiveMemoizesAndHoldsPrivateObjectsUntilClosed(t *testing.T) {
	repo, rc := memoryRepo(), &composeRecorder{}
	store := ObjectStore{Dir: t.Name(), Git: repo.git, Compose: rc.compose}
	ctx := context.Background()
	first, err := Effective(ctx, store, tid('1'))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Effective(ctx, store, tid('1'))
	if err != nil || rc.calls.Load() != 1 || second.ID != first.ID {
		t.Fatalf("second call = %+v, %v; compose calls %d, want 1", second, err, rc.calls.Load())
	}
	// Push the entry out of the LRU while both handles are held.
	var others []EffectiveTree
	for i := range memoCapacity {
		e, err := Effective(ctx, store, BaseTree(fmt.Sprintf("%040x", i+100)))
		if err != nil {
			t.Fatal(err)
		}
		others = append(others, e)
	}
	if rc.closed.Load() != 0 {
		t.Fatalf("evicted entry was released while held: closed %d", rc.closed.Load())
	}
	first.Close()
	first.Close() // idempotent: one handle, one reference
	if rc.closed.Load() != 0 {
		t.Fatal("released while the second handle still reads it")
	}
	second.Close()
	if rc.closed.Load() != 1 {
		t.Fatalf("closed = %d, want exactly the evicted, unreferenced entry", rc.closed.Load())
	}
	for _, e := range others {
		e.Close()
	}
	if got, err := Effective(ctx, store, tid('1')); err != nil || rc.calls.Load() != int32(memoCapacity)+2 {
		t.Fatalf("an evicted base composes again: %+v %v calls %d", got, err, rc.calls.Load())
	} else {
		got.Close()
	}
}

func TestEffectiveRecomposesWhenThePrivateObjectsAreGone(t *testing.T) {
	repo, rc := memoryRepo(), &composeRecorder{}
	var gone atomic.Bool
	store := ObjectStore{Dir: t.Name(), Compose: rc.compose, Git: func(ctx context.Context, d string, args ...string) ([]byte, error) {
		if gone.Load() && len(GitEnvFrom(ctx)) > 0 {
			return nil, errors.New("alternate removed")
		}
		return repo.git(ctx, d, args...)
	}}
	first, err := Effective(context.Background(), store, tid('2'))
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	gone.Store(true)
	second, err := Effective(context.Background(), store, tid('2'))
	if err != nil || rc.calls.Load() != 2 {
		t.Fatalf("stale entry must be rebuilt: %v, compose calls %d", err, rc.calls.Load())
	}
	second.Close()
	if rc.closed.Load() != 1 {
		t.Fatalf("the stale entry's objects are released once: %d", rc.closed.Load())
	}
}

func TestEffectiveClassifiesFailures(t *testing.T) {
	ctx := context.Background()
	conflict := fmt.Errorf("apply: %w", &patchset.PatchError{Kind: patchset.KindConflict, Patch: "0001-fix.patch", Detail: "context"})
	for name, tc := range map[string]struct {
		err  error
		want error
	}{
		"conflict is permanent for the commit": {conflict, ErrSourceInvalid},
		"infrastructure is transient":          {errors.New("git exploded"), ErrSourceUnavailable},
		"cancellation is transient":            {context.Canceled, ErrSourceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			rc := &composeRecorder{err: tc.err}
			_, err := Effective(ctx, ObjectStore{Dir: t.Name(), Git: memoryRepo().git, Compose: rc.compose}, tid('3'))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			var pe *patchset.PatchError
			if tc.err == conflict && (!errors.As(err, &pe) || pe.Kind != patchset.KindConflict) {
				t.Fatalf("the typed PatchError must survive for the hold decision: %v", err)
			}
		})
	}
	t.Run("malformed patch", func(t *testing.T) {
		repo := memoryRepo()
		repo.blobs[strings.Repeat("1", 40)] = "not a patch"
		_, err := Effective(ctx, ObjectStore{Dir: t.Name(), Git: repo.git, Compose: (&composeRecorder{}).compose}, tid('4'))
		var pe *patchset.PatchError
		if !errors.Is(err, ErrSourceInvalid) || !errors.As(err, &pe) || pe.Kind != patchset.KindMalformed {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a patch entry that is not a regular file", func(t *testing.T) {
		repo := memoryRepo()
		repo.dirs[patchset.Directory] = []string{rec("tree", patchset.Directory+"/0001-dir")}
		rc := &composeRecorder{}
		_, err := Effective(ctx, ObjectStore{Dir: t.Name(), Git: repo.git, Compose: rc.compose}, tid('5'))
		if !errors.Is(err, ErrSourceInvalid) || rc.calls.Load() != 0 {
			t.Fatalf("err = %v, compose calls %d", err, rc.calls.Load())
		}
	})
}
