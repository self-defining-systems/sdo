package install

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"sdo.dev/operator/broker"
)

var seedEvidenceName = regexp.MustCompile(`^[0-9a-f]{64}\.json$`)

// seedBrokerArchive snapshots only finalized deployment records from a trusted
// operator directory. It never imports proposals, incident receipts or locks.
// Validate the snapshot against the accepted Git history before touching the PVC.
func seedBrokerArchive(ctx context.Context, repository, revision, root string) ([]byte, error) {
	if root == "" {
		return nil, nil
	}
	if !filepath.IsAbs(root) {
		return nil, errors.New("trusted broker root must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	repo, err := filepath.Abs(repository)
	if err != nil {
		return nil, err
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		return nil, err
	}
	inside := func(parent, child string) bool {
		return parent == child || strings.HasPrefix(child, parent+string(filepath.Separator))
	}
	if inside(repo, resolved) || inside(resolved, repo) {
		return nil, errors.New("trusted seed broker and accepted repository must be separate")
	}
	directory := filepath.Join(resolved, "source-evidence")
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("source-evidence must be a regular directory")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 || len(entries) > 64 {
		return nil, errors.New("trusted seed requires 1-64 deployment records")
	}
	snapshot, err := os.MkdirTemp("", "sdo-seed-evidence-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(snapshot)
	if err := os.Mkdir(filepath.Join(snapshot, "source-evidence"), 0700); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	archive := tar.NewWriter(&buffer)
	total := 0
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !seedEvidenceName.MatchString(entry.Name()) || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return nil, errors.New("invalid trusted deployment record file")
		}
		file, err := os.Open(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, 1<<20+1))
		closeErr := file.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		total += len(data)
		if len(data) > 1<<20 || total > 8<<20 {
			return nil, errors.New("trusted deployment evidence exceeds size limit")
		}
		name := filepath.Join("source-evidence", entry.Name())
		if err := os.WriteFile(filepath.Join(snapshot, name), data, 0600); err != nil {
			return nil, err
		}
		if err := archive.WriteHeader(&tar.Header{Name: filepath.ToSlash(name), Mode: 0600, Size: int64(len(data))}); err != nil {
			return nil, err
		}
		if _, err := archive.Write(data); err != nil {
			return nil, err
		}
	}
	authority, err := broker.New(repo, snapshot, nil)
	if err != nil {
		return nil, err
	}
	proofs, err := authority.RenderedDeploymentEvidence(ctx, revision)
	if err != nil {
		return nil, fmt.Errorf("validate deployment records: %w", err)
	}
	if len(proofs) == 0 {
		return nil, errors.New("no trusted named image evidence matches the accepted seed")
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
