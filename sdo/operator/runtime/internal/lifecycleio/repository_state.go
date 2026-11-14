package lifecycleio

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"sdo.dev/operator/provenance"
)

// RepositoryState reads a checkout's HEAD and whether its working tree differs
// from it. A differing tree is captured as a commit object (never moved onto a
// ref) built from a private copy of the index, so uncommitted and untracked
// work is named without touching the checkout.
func RepositoryState(ctx context.Context, repository string) (provenance.Repository, error) {
	head, err := GitOutput(ctx, repository, "rev-parse", "HEAD")
	if err != nil {
		return provenance.Repository{}, err
	}
	state := provenance.Repository{Commit: strings.TrimSpace(string(head))}
	status, err := GitOutput(ctx, repository, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return provenance.Repository{}, err
	}
	if strings.TrimSpace(string(status)) == "" {
		return state, nil
	}
	state.Dirty = true
	state.Snapshot, _ = snapshotCommit(ctx, repository, state.Commit)
	return state, nil
}

func snapshotCommit(ctx context.Context, repository, head string) (string, error) {
	directory, err := os.MkdirTemp("", "sdo-snapshot-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(directory)
	indexPath, err := GitOutput(ctx, repository, "rev-parse", "--path-format=absolute", "--git-path", "index")
	if err != nil {
		return "", err
	}
	index := filepath.Join(directory, "index")
	if data, err := os.ReadFile(strings.TrimSpace(string(indexPath))); err == nil {
		if err := os.WriteFile(index, data, 0o600); err != nil {
			return "", err
		}
	}
	run := func(args ...string) (string, error) {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...)
		command.Env = append(os.Environ(), "GIT_INDEX_FILE="+index,
			"GIT_AUTHOR_NAME=sdo", "GIT_AUTHOR_EMAIL=sdo@localhost", "GIT_COMMITTER_NAME=sdo", "GIT_COMMITTER_EMAIL=sdo@localhost")
		output, err := command.Output()
		return strings.TrimSpace(string(output)), err
	}
	if _, err := run("add", "-A"); err != nil {
		return "", err
	}
	tree, err := run("write-tree")
	if err != nil {
		return "", err
	}
	return run("commit-tree", tree, "-p", head, "-m", "sdo working-tree snapshot")
}
