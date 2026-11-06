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
	// Env is added to the git process environment: an effective tree's
	// private object directory and alternates. Empty for accepted stores.
	Env []string
	// Compose replaces patch application plus projection (the real one runs
	// git). A simulation or unit test supplies results without a process.
	Compose ComposeFunc
}

type gitEnvKey struct{}

// GitEnvFrom returns the extra environment a GitFunc must give its git process
// when it wraps a real one: an effective tree's private object directory. It
// is empty for accepted stores, so a GitFunc that ignores it is only correct
// where no patch or projection wrote objects.
func GitEnvFrom(ctx context.Context) []string {
	env, _ := ctx.Value(gitEnvKey{}).([]string)
	return env
}

func (o ObjectStore) git(ctx context.Context, args ...string) ([]byte, error) {
	if o.Git != nil {
		if len(o.Env) > 0 {
			ctx = context.WithValue(ctx, gitEnvKey{}, o.Env)
		}
		return o.Git(ctx, o.Dir, args...)
	}
	return renderio.GitEnv(ctx, o.Dir, o.Env, args...)
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
