package renderio

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Extract writes the files of `tree` into dest (created, must not exist)
// from `git archive`, reading objects through env (an effective tree's private
// object directory). Entries that would leave dest, write through a symlink
// or are not regular files, directories or symlinks are rejected.
func Extract(ctx context.Context, root string, env []string, tree, dest string) error {
	if err := os.Mkdir(dest, 0o755); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "git", "-C", root, "archive", "--format=tar", tree)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	extractErr := ExtractTar(stdout, dest)
	_, _ = io.Copy(io.Discard, stdout)
	waitErr := cmd.Wait()
	if extractErr != nil {
		return extractErr
	}
	if waitErr != nil {
		return fmt.Errorf("git archive %s: %w: %s", tree, waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// ExtractTar unpacks a tar stream below dest with the safety rules of Extract.
func ExtractTar(r io.Reader, dest string) error {
	reader := tar.NewReader(r)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		name := strings.TrimSuffix(header.Name, "/")
		if name == "" || name == "." {
			continue
		}
		if filepath.IsAbs(name) || !filepath.IsLocal(filepath.FromSlash(name)) {
			return fmt.Errorf("archive entry %q leaves the destination", header.Name)
		}
		target := filepath.Join(dest, filepath.FromSlash(name))
		if err := noSymlinkParents(dest, filepath.Dir(target)); err != nil {
			return fmt.Errorf("archive entry %q: %w", header.Name, err)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			mode := os.FileMode(0o644)
			if header.Mode&0o111 != 0 {
				mode = 0o755
			}
			file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, reader); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			if err := os.Chmod(target, mode); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("archive entry %q has unsupported type %q", header.Name, header.Typeflag)
		}
	}
}

func noSymlinkParents(dest, dir string) error {
	rel, err := filepath.Rel(dest, dir)
	if err != nil {
		return err
	}
	current := dest
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("writes through a symbolic link")
		}
	}
	return nil
}

// Publish renames the finished tmp directory to dir. It reports whether dir
// already existed (another process finished the same content first), in which
// case tmp is removed.
func Publish(tmp, dir string) (existed bool, err error) {
	if err := os.Rename(tmp, dir); err != nil {
		if _, statErr := os.Stat(dir); statErr == nil {
			_ = os.RemoveAll(tmp)
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// Exists reports whether dir is a directory and marks it as just used.
func Exists(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return false
	}
	now := time.Now()
	_ = os.Chtimes(dir, now, now)
	return true
}

// Prune removes materialized directories under root that are not held, are
// not among the `keep` most recently used, and were last used longer than
// minAge ago. Names starting with "." are abandoned partial extractions and
// are removed after minAge. Failures to remove one directory are returned
// after the others were tried.
func Prune(root string, held map[string]bool, keep int, minAge time.Duration) error {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	type candidate struct {
		name string
		used time.Time
	}
	var done []candidate
	var errs []error
	now := time.Now()
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), ".") {
			if now.Sub(info.ModTime()) >= minAge {
				if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
					errs = append(errs, err)
				}
			}
			continue
		}
		done = append(done, candidate{entry.Name(), info.ModTime()})
	}
	sort.Slice(done, func(i, j int) bool { return done[i].used.After(done[j].used) })
	for index, c := range done {
		if index < keep || held[c.name] || now.Sub(c.used) < minAge {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, c.name)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
