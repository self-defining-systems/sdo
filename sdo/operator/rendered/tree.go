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
	// MkTree replaces the tree-object write Effective makes (`git mktree` of
	// one ls-tree level); a simulation supplies the ID without a process.
	MkTree func(ctx context.Context, dir string, records []string) (string, error)
}

func (o ObjectStore) git(ctx context.Context, args ...string) ([]byte, error) {
	if o.Git != nil {
		return o.Git(ctx, o.Dir, args...)
	}
	return renderio.Git(ctx, o.Dir, args...)
}

func (o ObjectStore) mktree(ctx context.Context, records []string) (string, error) {
	if o.MkTree != nil {
		return o.MkTree(ctx, o.Dir, records)
	}
	return renderio.MkTree(ctx, o.Dir, records)
}

var treeID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

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
