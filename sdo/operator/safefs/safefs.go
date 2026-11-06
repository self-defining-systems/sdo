// Package safefs is the one way Go writes into a tree an agent controls (a
// proposal or scratch checkout). The agent can plant symlinks anywhere in such
// a tree, so a plain os.WriteFile or os.MkdirAll from trusted Go code can be
// steered to overwrite any file the operator user owns. Every function here
// refuses a symlink in any component of the path (including the final one),
// stays inside the root through os.Root, and replaces files by renaming a
// temporary file created in the same directory, so an existing link or file is
// never written through.
package safefs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ErrSymlink reports that a path component is a symbolic link.
var ErrSymlink = errors.New("path traverses a symbolic link")

// SymlinkError names the link that was refused.
type SymlinkError struct {
	Root string
	Path string // slash-separated, relative to Root
}

func (e *SymlinkError) Error() string {
	return fmt.Sprintf("safefs: %s in %s is a symbolic link; Go does not write through links in an agent-controlled checkout", e.Path, e.Root)
}

func (e *SymlinkError) Unwrap() error { return ErrSymlink }

// Rel returns the slash-separated path of abs below root, for callers that
// hold a joined path.
func Rel(root, abs string) (string, error) {
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("safefs: %s is outside %s", abs, root)
	}
	return filepath.ToSlash(rel), nil
}

func clean(rel string) (string, error) {
	rel = filepath.ToSlash(rel)
	if rel == "" || rel == "." {
		return ".", nil
	}
	if !fs.ValidPath(rel) {
		return "", fmt.Errorf("safefs: invalid path %q", rel)
	}
	return rel, nil
}

// checkComponents Lstats every existing component of rel below root and
// refuses links. A missing component ends the walk (nothing below it exists).
func checkComponents(root, rel string) error {
	if info, err := os.Lstat(root); errors.Is(err, fs.ErrNotExist) {
		return nil // nothing below a missing root can be a link; writers create it
	} else if err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("safefs: root %s is not a directory", root)
	}
	if rel == "." {
		return nil
	}
	cur := ""
	for _, part := range strings.Split(rel, "/") {
		cur = path.Join(cur, part)
		info, err := os.Lstat(filepath.Join(root, filepath.FromSlash(cur)))
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return &SymlinkError{Root: root, Path: cur}
		}
	}
	return nil
}

// MkdirAll creates rel and its parents below root, refusing links.
func MkdirAll(root, rel string, perm fs.FileMode) error {
	rel, err := clean(rel)
	if err != nil {
		return err
	}
	if err := checkComponents(root, rel); err != nil {
		return err
	}
	r, err := openCreatingRoot(root)
	if err != nil {
		return err
	}
	defer r.Close()
	if rel == "." {
		return nil
	}
	return r.MkdirAll(rel, perm)
}

// WriteFile atomically replaces the regular file rel below root with data.
// Parent directories are created. The result has exactly mode's permissions.
func WriteFile(root, rel string, data []byte, mode fs.FileMode) error {
	rel, err := clean(rel)
	if err != nil {
		return err
	}
	if rel == "." {
		return fmt.Errorf("safefs: cannot write the root %s as a file", root)
	}
	if err := checkComponents(root, rel); err != nil {
		return err
	}
	r, err := openCreatingRoot(root)
	if err != nil {
		return err
	}
	defer r.Close()
	dir := path.Dir(rel)
	if dir != "." {
		if err := r.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if info, err := r.Lstat(rel); err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("safefs: %s in %s is not a regular file", rel, root)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	tmp, err := createTemp(r, dir)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = r.Remove(tmp)
		}
	}()
	f, err := r.OpenFile(tmp, os.O_WRONLY|os.O_EXCL, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(mode.Perm()); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := r.Rename(tmp, rel); err != nil {
		return err
	}
	committed = true
	return nil
}

func createTemp(r *os.Root, dir string) (string, error) {
	for i := 0; i < 100; i++ {
		name := path.Join(dir, fmt.Sprintf(".safefs-%d-%d.tmp", os.Getpid(), nextTemp()))
		f, err := r.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
		return name, nil
	}
	return "", errors.New("safefs: no free temporary file name")
}

// Remove deletes the file or empty directory rel below root, refusing links in
// any parent component. A missing path is not an error.
func Remove(root, rel string) error {
	rel, err := clean(rel)
	if err != nil {
		return err
	}
	if rel == "." {
		return fmt.Errorf("safefs: cannot remove the root %s", root)
	}
	if err := checkComponents(root, path.Dir(rel)); err != nil {
		return err
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := r.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// RemoveAll deletes rel and everything below it, refusing links in any parent
// component. A link at rel itself is removed, never followed. A missing path is
// not an error.
func RemoveAll(root, rel string) error {
	rel, err := clean(rel)
	if err != nil {
		return err
	}
	if rel == "." {
		return fmt.Errorf("safefs: cannot remove the root %s", root)
	}
	if err := checkComponents(root, path.Dir(rel)); err != nil {
		return err
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer r.Close()
	return r.RemoveAll(rel)
}

// openCreatingRoot opens root, creating it first when absent. The root path is
// the caller's own (an operator-chosen checkout location), never agent input.
func openCreatingRoot(root string) (*os.Root, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return os.OpenRoot(root)
}
