// Package gitpoll is the upstream watcher (ID "git") for a branch of an
// unauthenticated Git remote: Latest asks the remote for the branch tip and
// Materialize fetches that commit into the caller's object store.
package gitpoll

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/upstream"
	"sdo.dev/operator/upstream/gitpoll/internal/gitio"
)

// ID is the catalog ID of this watcher (the `kind` in sources.yaml).
const ID = "git"

var (
	commitID = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

// Watcher follows a Git branch through the git binary. The zero value is ready.
type Watcher struct{}

// Register adds the watcher to c under ID.
func Register(c *upstream.Catalog) error {
	return upstream.Register[*v1alpha1.GitPollConfig](c, ID, Watcher{})
}

// Validate checks a config statically.
func Validate(cfg *v1alpha1.GitPollConfig) error {
	if cfg.GetRepository() == "" || strings.HasPrefix(cfg.GetRepository(), "-") {
		return fmt.Errorf("gitpoll: repository %q must be a URL or path", cfg.GetRepository())
	}
	ref := cfg.GetRef()
	if ref == "" || strings.HasPrefix(ref, "-") || strings.ContainsAny(ref, " \t\n:?*[\\^~") || strings.Contains(ref, "..") {
		return fmt.Errorf("gitpoll: ref %q must be a branch name", ref)
	}
	if cfg.GetPollInterval() != nil {
		if err := cfg.GetPollInterval().CheckValid(); err != nil || cfg.GetPollInterval().AsDuration() < 0 {
			return fmt.Errorf("gitpoll: poll_interval %v is not a non-negative duration", cfg.GetPollInterval())
		}
	}
	return nil
}

// Latest implements upstream.Watcher.
func (Watcher) Latest(ctx context.Context, cfg *v1alpha1.GitPollConfig) (upstream.Revision, error) {
	if err := Validate(cfg); err != nil {
		return upstream.Revision{}, err
	}
	out, err := git(ctx, "", true, "ls-remote", "--", cfg.GetRepository(), "refs/heads/"+cfg.GetRef())
	if err != nil {
		return upstream.Revision{}, err
	}
	want := "refs/heads/" + cfg.GetRef()
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == want && commitID.MatchString(fields[0]) {
			return upstream.Revision{ID: fields[0], Ref: cfg.GetRef()}, nil
		}
	}
	return upstream.Revision{}, fmt.Errorf("gitpoll: %s has no branch %q", cfg.GetRepository(), cfg.GetRef())
}

// Materialize implements upstream.Watcher: it fetches rev into gitDir under a
// private ref (so the object is not collected) and verifies it is a commit.
func (Watcher) Materialize(ctx context.Context, cfg *v1alpha1.GitPollConfig, rev upstream.Revision, gitDir string) (string, error) {
	if err := Validate(cfg); err != nil {
		return "", err
	}
	if !commitID.MatchString(rev.ID) {
		return "", fmt.Errorf("gitpoll: revision %q is not a full commit ID", rev.ID)
	}
	if gitDir == "" {
		return "", errors.New("gitpoll: gitDir is required")
	}
	if _, err := git(ctx, gitDir, true, "fetch", "--no-tags", "--quiet", "--", cfg.GetRepository(), rev.ID+":"+upstream.HoldRefPrefix+rev.ID); err != nil {
		return "", err
	}
	out, err := git(ctx, gitDir, false, "rev-parse", "--verify", "--quiet", rev.ID+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("gitpoll: fetched %s is not a commit: %w", rev.ID, err)
	}
	if got := strings.TrimSpace(out); got != rev.ID {
		return "", fmt.Errorf("gitpoll: fetched %s resolved to %s", rev.ID, got)
	}
	return rev.ID, nil
}

// permanent are stderr fragments of a reachable remote that does not have what
// was asked for, which retrying cannot fix.
var permanent = []string{"not our ref", "couldn't find remote ref", "no such remote ref", "bad object"}

// git runs one git command, with gitDir as --git-dir when set. When network is
// set a failure wraps upstream.ErrUnavailable unless the remote answered that
// it lacks the object or is not a repository. A cancelled context returns the
// context error.
func git(ctx context.Context, gitDir string, network bool, args ...string) (string, error) {
	stdout, stderr, err := gitio.Run(ctx, gitDir, args...)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		msg := strings.TrimSpace(stderr)
		if network && !containsAny(msg, permanent) {
			return "", fmt.Errorf("gitpoll: git %s: %w: %v: %s", args[0], upstream.ErrUnavailable, err, msg)
		}
		return "", fmt.Errorf("gitpoll: git %s: %v: %s", args[0], err, msg)
	}
	return stdout, nil
}

func containsAny(s string, parts []string) bool {
	for _, p := range parts {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
