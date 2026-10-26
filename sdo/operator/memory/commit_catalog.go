package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// CommitCatalogReader reads immutable accepted bytes while peer controllers
// publish newer heads. Each dispatched task remains bound to its proposal base.
type CommitCatalogReader interface {
	CatalogReader
	SnapshotAt(context.Context, string) (*contracts.OperationalMemorySnapshot, error)
}

// SnapshotAt requests the exact proposal revision. Readers without historical
// storage retain the equality check rather than silently returning newer bytes.
func SnapshotAt(ctx context.Context, reader CatalogReader, commit string) (*contracts.OperationalMemorySnapshot, error) {
	if reader == nil {
		return nil, errors.New("operational memory reader is required")
	}
	if pinned, ok := reader.(CommitCatalogReader); ok {
		return pinned.SnapshotAt(ctx, commit)
	}
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.GetAcceptedCommit() != commit {
		return nil, errors.New("memory snapshot differs from broker proposal base commit")
	}
	return snapshot, nil
}

var fullCommitPattern = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

func (s *JSONStore) SnapshotAt(ctx context.Context, commit string) (*contracts.OperationalMemorySnapshot, error) {
	if s == nil || s.root == "" {
		return nil, errors.New("accepted repository is required")
	}
	if !fullCommitPattern.MatchString(commit) {
		return nil, errors.New("memory snapshot requires a full accepted commit")
	}
	directory, err := os.MkdirTemp("", "sdo-catalog-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	// A private index and checkout read objects directly without the mutable
	// accepted worktree/index, archive attributes, hardlinks, or a publishing remote.
	index := filepath.Join(directory, "index")
	run := func(input []byte, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = s.root
		cmd.Env = append(os.Environ(), "GIT_INDEX_FILE="+index)
		cmd.Stdin = bytes.NewReader(input)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("accepted memory git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return output, nil
	}
	if _, err := run(nil, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
		return nil, fmt.Errorf("snapshot commit is not accepted: %w", err)
	}
	if _, err := run(nil, "read-tree", commit); err != nil {
		return nil, err
	}
	paths, err := run(nil, "ls-tree", "-r", "--name-only", "-z", commit, "--", layout.Dir)
	if err != nil {
		return nil, err
	}
	var selected bytes.Buffer
	for _, path := range strings.Split(string(paths), "\x00") {
		// These storage classes are not part of the logical catalog. In particular,
		// do not copy unbounded historical corpus and outcome data for every task.
		if path == "" || strings.HasPrefix(path, layout.CorpusDir+"/") || strings.HasPrefix(path, layout.OutcomesDir+"/") || strings.HasPrefix(path, layout.RenderedDir+"/") {
			continue
		}
		selected.WriteString(path)
		selected.WriteByte(0)
	}
	root := filepath.Join(directory, "checkout")
	if err := os.Mkdir(root, 0700); err != nil {
		return nil, err
	}
	if _, err := run(selected.Bytes(), "checkout-index", "--stdin", "-z", "--prefix="+root+string(filepath.Separator)); err != nil {
		return nil, err
	}
	return CandidateSnapshot(root, commit)
}
