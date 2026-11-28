package rendered

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// projectedRoots are the root entries of a commit's tree that are never
// deployed: operational memory and the agents' runtime directory.
var projectedRoots = map[string]bool{layout.Dir: true, ".sdo-runtime": true}

// EffectiveTree is what renderers, builds and receipts see: the commit's tree
// without its .sdo/ and .sdo-runtime/ entries. Store is the object store that
// holds it (the base tree's own).
type EffectiveTree struct {
	ID    string
	Store ObjectStore
}

// Effective projects .sdo/ and .sdo-runtime/ away from base, so a memory-only
// commit never changes the deployed tree ID, the source stamp or a render
// cache key. A tree without either entry is its own effective tree (nothing is
// written); otherwise the projected root is written once into the base's own
// object store (one ls-tree of the root and one mktree).
//
// Errors are *SourceError: I/O and cancellation are Unavailable.
func Effective(ctx context.Context, objects ObjectStore, base BaseTree) (EffectiveTree, error) {
	id := string(base)
	if !treeID.MatchString(id) {
		return EffectiveTree{}, fmt.Errorf("base tree %q is not a Git object ID", id)
	}
	if err := requireTree(ctx, objects, id); err != nil {
		return EffectiveTree{}, err
	}
	out, err := objects.git(ctx, "ls-tree", "-z", "--full-tree", id)
	if err != nil {
		return EffectiveTree{}, sourceErrorf(SourceUnavailable, "list the root of %s: %w", id, err)
	}
	var kept []string
	dropped := false
	for _, record := range bytes.Split(out, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		_, name, ok := strings.Cut(string(record), "\t")
		if !ok {
			return EffectiveTree{}, sourceErrorf(SourceUnavailable, "malformed ls-tree record %q", record)
		}
		if projectedRoots[name] {
			dropped = true
			continue
		}
		kept = append(kept, string(record))
	}
	if !dropped {
		return EffectiveTree{ID: id, Store: objects}, nil
	}
	projected, err := objects.mktree(ctx, kept)
	if err != nil {
		return EffectiveTree{}, sourceErrorf(SourceUnavailable, "write the projection of %s: %w", id, err)
	}
	if !treeID.MatchString(projected) {
		return EffectiveTree{}, fmt.Errorf("effective tree %q is not a Git object ID", projected)
	}
	return EffectiveTree{ID: projected, Store: objects}, nil
}

// ChangedSince lists the tree-relative files that differ between the earlier
// effective tree and t (added, removed or modified; a rename is both paths).
// An earlier tree the store no longer holds is an error.
func (t EffectiveTree) ChangedSince(ctx context.Context, earlier string) ([]string, error) {
	if !treeID.MatchString(earlier) {
		return nil, fmt.Errorf("earlier tree %q is not a Git object ID", earlier)
	}
	if earlier == t.ID {
		return nil, nil
	}
	if err := requireTree(ctx, t.Store, earlier); err != nil {
		return nil, err
	}
	out, err := t.Store.git(ctx, "diff-tree", "-r", "-z", "--no-renames", "--name-only", earlier, t.ID)
	if err != nil {
		return nil, sourceErrorf(SourceUnavailable, "diff effective trees %s and %s: %w", earlier, t.ID, err)
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths, nil
}
