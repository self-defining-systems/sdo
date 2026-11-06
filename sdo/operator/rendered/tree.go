package rendered

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"sdo.dev/operator/rendered/internal/renderio"
)

// BaseTree is the commit or proposal tree that receipts and the
// sdo.dev/source-tree stamp bind (VerifyAccepted compares commit^{tree}).
type BaseTree string

// EffectiveTree is what renderers see: BaseTree plus .sdo/deployment patches
// (slice 5). Only Effective produces one, so no path can skip the seam.
type EffectiveTree string

// GitFunc is the git process edge: standard output of `git -C dir args...`.
// A simulation supplies trees without spawning a process.
type GitFunc func(ctx context.Context, dir string, args ...string) ([]byte, error)

// ObjectStore names the repository whose object database holds a tree. Deploy
// passes the candidate checkout, because proposal-tree objects do not exist in
// the accepted root; reads pass the accepted root.
type ObjectStore struct {
	Dir string
	// Git replaces the git process edge when set.
	Git GitFunc
}

func (o ObjectStore) git(ctx context.Context, args ...string) ([]byte, error) {
	if o.Git != nil {
		return o.Git(ctx, o.Dir, args...)
	}
	return renderio.Git(ctx, o.Dir, args...)
}

var treeID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

// Effective is the single "base + patches" seam. Slice 3 is the identity
// conversion after checking that the object store holds a tree with that ID;
// slice 5 applies .sdo/deployment patches here.
func Effective(ctx context.Context, objects ObjectStore, base BaseTree) (EffectiveTree, error) {
	id := string(base)
	if !treeID.MatchString(id) {
		return "", fmt.Errorf("base tree %q is not a Git object ID", id)
	}
	if err := requireTree(ctx, objects, id); err != nil {
		return "", err
	}
	return EffectiveTree(id), nil
}

func requireTree(ctx context.Context, objects ObjectStore, id string) error {
	out, err := objects.git(ctx, "cat-file", "-t", id)
	if err != nil {
		return sourceErrorf(SourceUnavailable, "%s in %s: %w", id, objects.Dir, err)
	}
	if kind := strings.TrimSpace(string(out)); kind != "tree" {
		return fmt.Errorf("%s is a %s, not a tree", id, kind)
	}
	return nil
}
