package shellpath

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sdo.dev/operator/shellpath/internal/shellio"
	"strings"
)

var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}(?:[0-9a-f]{24})?$`)

// GitRepository pulls immutable accepted bytes into private /tmp. Agents never
// edit this checkout; activation swaps the complete script set at a commit boundary.
type GitRepository struct {
	Root, Scratch string
	current       Snapshot
}

func (r *GitRepository) Current(ctx context.Context) (Snapshot, error) {
	output, err := shellio.Head(ctx, r.Root)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read accepted revision: %w", err)
	}
	commit := strings.TrimSpace(string(output))
	if !commitPattern.MatchString(commit) {
		return Snapshot{}, errors.New("invalid accepted revision")
	}
	if commit == r.current.Commit {
		return r.current, nil
	}
	directory, err := os.MkdirTemp(r.Scratch, "sdo-shell-")
	if err != nil {
		return Snapshot{}, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(directory)
		}
	}()
	if err = shellio.Archive(ctx, r.Root, commit, func(stream io.Reader) error { return extractArchive(directory, tar.NewReader(stream)) }); err != nil {
		return Snapshot{}, err
	}
	scripts, err := Discover(directory)
	if err != nil {
		return Snapshot{}, err
	}
	previous := r.current.Root
	r.current = Snapshot{Commit: commit, Root: directory, Scripts: scripts}
	keep = true
	if previous != "" {
		_ = os.RemoveAll(previous)
	}
	return r.current, nil
}
func extractArchive(root string, reader *tar.Reader) error {
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeXGlobalHeader || header.Typeflag == tar.TypeXHeader {
			continue
		}
		name := strings.TrimSuffix(header.Name, "/")
		if !filepath.IsLocal(filepath.FromSlash(name)) || strings.Contains(name, "\\") {
			return errors.New("unsafe accepted archive path")
		}
		target := filepath.Join(root, filepath.FromSlash(name))
		if header.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(target, 0700); err != nil {
				return err
			}
			continue
		}
		// Symlinks in source are not needed for detector execution and are never followed.
		if header.Typeflag == tar.TypeSymlink || header.Typeflag == tar.TypeLink {
			if strings.HasPrefix(name, ".sdo/") {
				return errors.New("symlink in accepted operational memory")
			}
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return errors.New("non-regular accepted archive entry")
		}
		if header.Size > 64<<20 {
			return errors.New("accepted archive file exceeds size cap")
		}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode)&0777)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(file, reader)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
}
