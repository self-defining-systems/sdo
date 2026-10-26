package memory

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
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
	// Read raw blobs rather than checkout-index/archive: checkout conversion
	// filters and newer worktree attributes cannot rewrite historical bytes.
	run := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = s.root
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("accepted memory git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return output, nil
	}
	if _, err := run("merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
		return nil, fmt.Errorf("snapshot commit is not accepted: %w", err)
	}
	paths, err := run("ls-tree", "-r", "-z", commit, "--", layout.Dir)
	if err != nil {
		return nil, err
	}
	var selected []catalogBlob
	for _, record := range strings.Split(string(paths), "\x00") {
		if record == "" {
			continue
		}
		fields := strings.SplitN(record, "\t", 2)
		if len(fields) != 2 {
			return nil, errors.New("invalid accepted memory tree entry")
		}
		metadata, path := strings.Fields(fields[0]), fields[1]
		if len(metadata) != 3 || !filepath.IsLocal(filepath.FromSlash(path)) || !layout.IsMemoryPath(path) {
			return nil, errors.New("unsafe accepted memory tree entry")
		}
		// Corpus, outcomes and rendered deployment evidence are not catalog
		// content and can grow without bound over the application's lifetime.
		if strings.HasPrefix(path, layout.CorpusDir+"/") || strings.HasPrefix(path, layout.OutcomesDir+"/") || strings.HasPrefix(path, layout.RenderedDir+"/") {
			continue
		}
		if metadata[1] != "blob" || (metadata[0] != "100644" && metadata[0] != "100755") {
			return nil, fmt.Errorf("operational memory requires regular files: %s", path)
		}
		selected = append(selected, catalogBlob{path: path, object: metadata[2]})
	}
	root := filepath.Join(directory, "checkout")
	if err := os.Mkdir(root, 0700); err != nil {
		return nil, err
	}
	if err := s.exportCatalogBlobs(ctx, root, selected); err != nil {
		return nil, err
	}
	return CandidateSnapshot(root, commit)
}

// catalogBlob names one immutable Git object and its validated relative path.
type catalogBlob struct{ path, object string }

func (s *JSONStore) exportCatalogBlobs(ctx context.Context, root string, blobs []catalogBlob) error {
	var input strings.Builder
	for _, blob := range blobs {
		input.WriteString(blob.object)
		input.WriteByte('\n')
	}
	command := exec.CommandContext(ctx, "git", "cat-file", "--batch")
	command.Dir = s.root
	command.Stdin = strings.NewReader(input.String())
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	reader := bufio.NewReader(output)
	for _, blob := range blobs {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read accepted memory blob %s: %w", blob.path, err)
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != blob.object || fields[1] != "blob" {
			return fmt.Errorf("invalid accepted memory blob %s", blob.path)
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 || size > layout.MaxArtifactBytes {
			return fmt.Errorf("accepted memory artifact exceeds size bound: %s", blob.path)
		}
		data := make([]byte, int(size)+1)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		if data[len(data)-1] != '\n' {
			return errors.New("invalid accepted memory blob delimiter")
		}
		target := filepath.Join(root, filepath.FromSlash(blob.path))
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(target, data[:len(data)-1], 0600); err != nil {
			return err
		}
	}
	if err := command.Wait(); err != nil {
		return fmt.Errorf("read accepted memory objects: %w: %s", err, stderr.String())
	}
	return nil
}
