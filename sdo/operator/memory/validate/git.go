package validate

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
)

func git(ctx context.Context, root string, args ...string) ([]byte, error) {
	out, stderr, err := memoryio.GitWithDiagnostics(ctx, root, args...)
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr))
	}
	return out, nil
}

func gitText(ctx context.Context, root string, args ...string) (string, error) {
	out, err := git(ctx, root, args...)
	return strings.TrimSpace(string(out)), err
}

// ChangedPaths is the actual tree diff of a checkout against a base revision,
// including local commits, staged changes, deletions, and untracked files. It
// never trusts claims made by the author of the change. Paths are
// slash-separated, repository-relative, and sorted.
func ChangedPaths(ctx context.Context, root, base string) ([]string, error) {
	modified, err := git(ctx, root, "-c", "diff.external=", "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z", base, "--")
	if err != nil {
		return nil, err
	}
	untracked, err := git(ctx, root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, err
	}
	ignored, err := git(ctx, root, "ls-files", "--others", "-z", "--", layout.LibrariesDir, layout.CorpusDir)
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, raw := range [][]byte{modified, untracked, ignored} {
		for _, part := range strings.Split(string(raw), "\x00") {
			if part != "" {
				set[part] = struct{}{}
			}
		}
	}
	paths := make([]string, 0, len(set))
	for p := range set {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths, nil
}

// exportBase extracts the base revision's operational memory into a temporary
// directory so the layout readers can parse it. The caller removes the
// directory. A base without memory yields an empty directory.
func exportBase(ctx context.Context, root, base string) (string, error) {
	dir, err := os.MkdirTemp("", "sdo-validate-base-")
	if err != nil {
		return "", err
	}
	listing, err := git(ctx, root, "ls-tree", "-r", "--name-only", "-z", base, "--", layout.Dir)
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	if len(bytes.TrimSpace(listing)) == 0 {
		return dir, nil
	}
	archive, err := git(ctx, root, "archive", "--format=tar", base, "--", layout.Dir)
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return dir, nil
		}
		if err != nil {
			os.RemoveAll(dir)
			return "", err
		}
		if header.Typeflag != tar.TypeReg || path.Clean(header.Name) != header.Name || !layout.IsMemoryPath(header.Name) {
			continue
		}
		target := filepath.Join(dir, filepath.FromSlash(header.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			os.RemoveAll(dir)
			return "", err
		}
		data, err := io.ReadAll(reader)
		if err == nil {
			err = os.WriteFile(target, data, 0o644)
		}
		if err != nil {
			os.RemoveAll(dir)
			return "", err
		}
	}
}

// exportCheckout checks the whole base revision out into a temporary directory
// through a private index, as a checkout of it would hold the files: modes,
// symlinks and checkout filters included, archive attributes not. The caller
// removes the directory.
func exportCheckout(ctx context.Context, root, base string) (string, error) {
	dir, err := os.MkdirTemp("", "sdo-validate-checkout-")
	if err != nil {
		return "", err
	}
	index, err := os.CreateTemp("", "sdo-validate-index-")
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	index.Close()
	os.Remove(index.Name())
	defer os.Remove(index.Name())
	env := []string{"GIT_INDEX_FILE=" + index.Name()}
	for _, args := range [][]string{{"read-tree", base}, {"checkout-index", "--all", "--force", "--prefix=" + dir + string(filepath.Separator)}} {
		if out, err := memoryio.Git(ctx, root, true, env, args...); err != nil {
			os.RemoveAll(dir)
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
	}
	return dir, nil
}
