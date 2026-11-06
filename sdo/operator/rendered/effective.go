package rendered

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
)

// Deployment-class layout as the effective tree sees it. The accepted
// repository keeps the whole class under .sdo/deployment; renderers and
// builds see only the source-shaped directories of it (the authored file
// directories and the overlay root), and `plan.yaml` and `patches/` are inputs
// of Effective, not of renderers.
const (
	sdoDir        = layout.Dir
	deploymentDir = layout.DeploymentDir
)

// deploymentViews are the directories of .sdo/deployment that stay in the
// effective tree (they are the renderer and build inputs).
var deploymentViews = func() map[string]bool {
	views := map[string]bool{"overlays": true}
	for _, dir := range layout.DeploymentFileDirs {
		views[dir] = true
	}
	return views
}()

// Composed is the result of applying a patch series to a base tree and
// projecting it: the effective tree ID, the environment under which git reads
// it (a private object directory), and its release.
type Composed struct {
	Tree  string
	Env   []string
	Close func()
}

// ComposeFunc applies series to base (read from the repository at dir) and
// writes the projection that drops the given paths. The real one is
// composeGit (patchset.Apply + a scratch index).
type ComposeFunc func(ctx context.Context, dir, base string, series patchset.Series, drop []string) (Composed, error)

// EffectiveTree is what renderers, builds and receipts see: the base tree with
// the .sdo/deployment patch series applied, minus every .sdo/ entry that is
// not a deployment view. It carries the object store that holds its objects
// (a private directory when anything was written), so GitTreeFS and
// `git archive` read it through Store.
//
// Only Effective produces one, so no path can skip the seam. Close releases
// the hold on the private objects; use it exactly where the tree stops being
// read (it is idempotent and safe for an identity tree).
type EffectiveTree struct {
	ID    string
	Store ObjectStore
	// Patches is the number of applied patches; PatchesDigest is
	// patchset.Series.Digest of them (the digest of the empty series when none).
	Patches       int
	PatchesDigest string
	release       func()
}

// Close releases the tree's hold on its private object directory.
func (t EffectiveTree) Close() {
	if t.release != nil {
		t.release()
	}
}

// Effective is the single "base + patches" seam.
//
// Stable under memory-only commits: playbooks, outcomes and detectors are
// projected away, so they never change the ID. A base tree that already equals
// its projection and has no patches is its own effective tree (no process
// writes, Store is the base store). Everything else is composed once per
// (store directory, base) and memoized with reference counting, so the private
// objects of a tree in use are never deleted by eviction.
//
// Errors are *SourceError: a *patchset.PatchError (reach it with errors.As) is
// Invalid, permanent for this commit; I/O and cancellation are Unavailable.
func Effective(ctx context.Context, objects ObjectStore, base BaseTree) (EffectiveTree, error) {
	id := string(base)
	if !treeID.MatchString(id) {
		return EffectiveTree{}, fmt.Errorf("base tree %q is not a Git object ID", id)
	}
	if err := requireTree(ctx, objects, id); err != nil {
		return EffectiveTree{}, err
	}
	drop, patchEntries, err := classifyBase(ctx, objects, id)
	if err != nil {
		return EffectiveTree{}, err
	}
	if len(drop) == 0 && len(patchEntries) == 0 {
		return EffectiveTree{ID: id, Store: objects, PatchesDigest: emptyDigest}, nil
	}
	key := memoKey{objects.Dir, id}
	return memo.get(ctx, key, objects, func() (*memoEntry, error) {
		series, err := readSeries(ctx, objects, patchEntries)
		if err != nil {
			return nil, err
		}
		compose := objects.Compose
		if compose == nil {
			compose = composeGit
		}
		composed, err := compose(ctx, objects.Dir, id, series, drop)
		if err != nil {
			return nil, classifyCompose(err)
		}
		if !treeID.MatchString(composed.Tree) {
			if composed.Close != nil {
				composed.Close()
			}
			return nil, fmt.Errorf("effective tree %q is not a Git object ID", composed.Tree)
		}
		store := objects
		store.Env = composed.Env
		return &memoEntry{id: composed.Tree, store: store, patches: series.Len(), digest: series.Digest(), cleanup: composed.Close}, nil
	})
}

var emptyDigest = func() string {
	s, _ := patchset.ParseSeries(nil)
	return s.Digest()
}()

// classifyBase lists the base tree's .sdo/ projection drops and its patch
// files, with at most three listings of single directories (never the memory
// files themselves).
func classifyBase(ctx context.Context, objects ObjectStore, tree string) (drop []string, patches []treeEntry, err error) {
	sdo, err := listDir(ctx, objects, tree, sdoDir)
	if err != nil {
		return nil, nil, err
	}
	hasDeployment := false
	for _, e := range sdo {
		if e.name == "deployment" && e.tree() {
			hasDeployment = true
			continue
		}
		drop = append(drop, e.path)
	}
	if !hasDeployment {
		return drop, nil, nil
	}
	dep, err := listDir(ctx, objects, tree, deploymentDir)
	if err != nil {
		return nil, nil, err
	}
	hasPatches := false
	for _, e := range dep {
		switch {
		case e.name == "patches" && e.tree():
			hasPatches = true
			drop = append(drop, e.path)
		case deploymentViews[e.name] && e.tree():
		default:
			drop = append(drop, e.path)
		}
	}
	if hasPatches {
		list, err := listDir(ctx, objects, tree, patchset.Directory)
		if err != nil {
			return nil, nil, err
		}
		patches = list
	}
	return drop, patches, nil
}

type treeEntry struct {
	mode, kind, blob, path, name string
}

func (e treeEntry) tree() bool { return e.kind == "tree" }

// listDir lists the direct children of dir in tree (no entry for a missing dir).
func listDir(ctx context.Context, objects ObjectStore, tree, dir string) ([]treeEntry, error) {
	out, err := objects.git(ctx, "ls-tree", "-z", "--full-tree", tree, dir+"/")
	if err != nil {
		return nil, sourceErrorf(SourceUnavailable, "list %s in %s: %w", dir, tree, err)
	}
	var entries []treeEntry
	for _, record := range bytes.Split(out, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		meta, name, ok := strings.Cut(string(record), "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 3 {
			return nil, sourceErrorf(SourceUnavailable, "malformed ls-tree record %q", record)
		}
		entries = append(entries, treeEntry{mode: fields[0], kind: fields[1], blob: fields[2], path: name, name: path.Base(name)})
	}
	return entries, nil
}

// readSeries reads the patch files (regular blobs only) and parses the series.
func readSeries(ctx context.Context, objects ObjectStore, entries []treeEntry) (patchset.Series, error) {
	files := make([]patchset.File, 0, len(entries))
	for _, e := range entries {
		if e.kind != "blob" || (e.mode != "100644" && e.mode != "100755") {
			return patchset.Series{}, sourceErrorf(SourceInvalid, "%s must be a regular file in the accepted tree", e.path)
		}
		data, err := objects.git(ctx, "cat-file", "blob", e.blob)
		if err != nil {
			return patchset.Series{}, sourceErrorf(SourceUnavailable, "read %s: %w", e.path, err)
		}
		files = append(files, patchset.File{Name: e.name, Data: data})
	}
	series, err := patchset.ParseSeries(files)
	if err != nil {
		return patchset.Series{}, classifyCompose(err)
	}
	return series, nil
}

func classifyCompose(err error) error {
	var existing *SourceError
	if errors.As(err, &existing) {
		return err
	}
	var patchErr *patchset.PatchError
	if errors.As(err, &patchErr) {
		return &SourceError{Kind: SourceInvalid, Err: err}
	}
	return &SourceError{Kind: SourceUnavailable, Err: err}
}

// memoKey identifies one composition: the base tree in one object store.
// A tree ID fixes its patch files, so the ID is the whole identity.
type memoKey struct{ dir, base string }

type memoEntry struct {
	id      string
	store   ObjectStore
	patches int
	digest  string
	cleanup func()
	// guarded by effectiveMemo.mu
	refs    int
	evicted bool
	elem    *list.Element
	key     memoKey
}

const memoCapacity = 16

// effectiveMemo is a process-wide LRU of composed trees. Entries are
// reference counted: eviction (or a failed liveness check) removes an entry
// from the table immediately but deletes its private objects only when the
// last holder calls Close.
type effectiveMemo struct {
	mu      sync.Mutex
	entries map[memoKey]*memoEntry
	order   *list.List
}

var memo = &effectiveMemo{entries: map[memoKey]*memoEntry{}, order: list.New()}

func (m *effectiveMemo) get(ctx context.Context, key memoKey, objects ObjectStore, compute func() (*memoEntry, error)) (EffectiveTree, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		// A recreated repository can leave a stale alternate behind; one
		// process proves the private objects still resolve.
		if err := requireTree(ctx, e.store, e.id); err == nil {
			m.order.MoveToFront(e.elem)
			return m.handle(e), nil
		} else if ctx.Err() != nil {
			return EffectiveTree{}, err
		}
		m.drop(e)
	}
	e, err := compute()
	if err != nil {
		return EffectiveTree{}, err
	}
	e.key = key
	e.elem = m.order.PushFront(e)
	m.entries[key] = e
	for m.order.Len() > memoCapacity {
		m.drop(m.order.Back().Value.(*memoEntry))
	}
	return m.handle(e), nil
}

// handle takes a reference; callers hold mu.
func (m *effectiveMemo) handle(e *memoEntry) EffectiveTree {
	e.refs++
	var once sync.Once
	return EffectiveTree{ID: e.id, Store: e.store, Patches: e.patches, PatchesDigest: e.digest, release: func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			e.refs--
			m.finish(e)
		})
	}}
}

// drop unlinks e; callers hold mu.
func (m *effectiveMemo) drop(e *memoEntry) {
	if !e.evicted {
		e.evicted = true
		delete(m.entries, e.key)
		m.order.Remove(e.elem)
	}
	m.finish(e)
}

func (m *effectiveMemo) finish(e *memoEntry) {
	if e.evicted && e.refs == 0 && e.cleanup != nil {
		e.cleanup()
		e.cleanup = nil
	}
}
