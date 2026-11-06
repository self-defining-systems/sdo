// Package render defines the renderer seam: a pure function from one Git-tree
// view, a recorded environment and a typed config to Kubernetes objects.
//
// This package imports only the standard library, protobuf and apimachinery.
// Renderer implementations live in sub-packages and import this one.
package render

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

// FS is a read-only view of one effective source tree (a Git tree object).
// Implementations expose only tracked regular files; symlinks, submodules and
// untracked files do not exist in it. Paths are slash-separated and relative.
type FS interface {
	fs.ReadFileFS
	fs.ReadDirFS
	fs.StatFS
	// Tree is the Git tree ID this view reads.
	Tree() string
}

// Limits bounds what one render may read.
type Limits struct {
	// FileBytes caps one file. TotalBytes caps the distinct files read.
	// Zero fields take the defaults.
	FileBytes, TotalBytes int64
}

// DefaultLimits are 2 MiB per file and 16 MiB per render.
var DefaultLimits = Limits{FileBytes: 2 << 20, TotalBytes: 16 << 20}

func (l Limits) withDefaults() Limits {
	if l.FileBytes <= 0 {
		l.FileBytes = DefaultLimits.FileBytes
	}
	if l.TotalBytes <= 0 {
		l.TotalBytes = DefaultLimits.TotalBytes
	}
	return l
}

// ErrLimit marks an input that exceeds Limits.
var ErrLimit = errors.New("render input exceeds limit")

// Recorder wraps an FS, enforces Limits and records every path read and
// listed. The catalog, never the renderer, turns the record into
// Result.Inputs.
type Recorder struct {
	fsys   FS
	limits Limits

	mu    sync.Mutex
	files map[string]int64 // path -> size, each distinct file counted once
	dirs  map[string]bool
	total int64
}

var _ FS = (*Recorder)(nil)

// Record wraps fsys. Zero Limits fields take DefaultLimits.
func Record(fsys FS, limits Limits) *Recorder {
	return &Recorder{fsys: fsys, limits: limits.withDefaults(), files: map[string]int64{}, dirs: map[string]bool{}}
}

// Tree returns the wrapped tree ID.
func (r *Recorder) Tree() string { return r.fsys.Tree() }

// Inputs lists the files read and the directories listed, sorted and
// deduplicated. A directory is spelled with a trailing slash ("./" is the
// root).
func (r *Recorder) Inputs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.files)+len(r.dirs))
	for p := range r.files {
		out = append(out, p)
	}
	for d := range r.dirs {
		out = append(out, strings.TrimSuffix(d, "/")+"/")
	}
	sort.Strings(out)
	return out
}

// ReadFile reads and records name, enforcing Limits.
func (r *Recorder) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrInvalid}
	}
	if info, err := r.fsys.Stat(name); err == nil && !info.IsDir() {
		if err := r.admit(name, info.Size()); err != nil {
			return nil, err
		}
	}
	data, err := r.fsys.ReadFile(name)
	if err != nil {
		return nil, err
	}
	if err := r.admit(name, int64(len(data))); err != nil {
		return nil, err
	}
	return data, nil
}

// admit charges size against the limits and records the file. A path already
// recorded with the same size is free.
func (r *Recorder) admit(name string, size int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if size > r.limits.FileBytes {
		return fmt.Errorf("%w: %s is %d bytes, file limit %d", ErrLimit, name, size, r.limits.FileBytes)
	}
	prev, seen := r.files[name]
	if seen && prev == size {
		return nil
	}
	if r.total-prev+size > r.limits.TotalBytes {
		return fmt.Errorf("%w: reading %s exceeds total limit %d", ErrLimit, name, r.limits.TotalBytes)
	}
	r.total += size - prev
	r.files[name] = size
	return nil
}

// ReadDir lists and records name.
func (r *Recorder) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	entries, err := r.fsys.ReadDir(name)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.dirs[name] = true
	r.mu.Unlock()
	return entries, nil
}

// Stat reports file metadata. It is not recorded as an input: it reveals
// existence and size only.
func (r *Recorder) Stat(name string) (fs.FileInfo, error) { return r.fsys.Stat(name) }

// Open opens name. Files are read whole through ReadFile (so they are
// recorded and limited); directories are listed through ReadDir.
func (r *Recorder) Open(name string) (fs.File, error) {
	return openVia(r, name)
}

// Overlay returns fsys with extra virtual files (chain hand-off, kustomize
// virtual Secret references). A virtual file shadows a real one. Reads of
// virtual files do not reach the wrapped FS, so a Recorder wrapped around
// fsys does not record them as inputs. Tree is the wrapped tree ID.
func Overlay(fsys FS, files map[string][]byte) FS {
	o := &overlay{FS: fsys, files: map[string][]byte{}}
	for name, data := range files {
		o.files[path.Clean(name)] = append([]byte(nil), data...)
	}
	return o
}

type overlay struct {
	FS
	files map[string][]byte
}

func (o *overlay) ReadFile(name string) ([]byte, error) {
	if data, ok := o.files[name]; ok {
		return append([]byte(nil), data...), nil
	}
	return o.FS.ReadFile(name)
}

func (o *overlay) Stat(name string) (fs.FileInfo, error) {
	if data, ok := o.files[name]; ok {
		return fileInfo{name: path.Base(name), size: int64(len(data))}, nil
	}
	if o.isVirtualDir(name) {
		if info, err := o.FS.Stat(name); err == nil {
			return info, nil
		}
		return fileInfo{name: path.Base(name), dir: true}, nil
	}
	return o.FS.Stat(name)
}

func (o *overlay) isVirtualDir(name string) bool {
	prefix := name + "/"
	if name == "." {
		prefix = ""
	}
	for p := range o.files {
		if strings.HasPrefix(p, prefix) && p != name {
			return true
		}
	}
	return false
}

func (o *overlay) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := o.FS.ReadDir(name)
	if err != nil && !(errors.Is(err, fs.ErrNotExist) && o.isVirtualDir(name)) {
		return nil, err
	}
	byName := map[string]fs.DirEntry{}
	for _, e := range entries {
		byName[e.Name()] = e
	}
	prefix := name + "/"
	if name == "." {
		prefix = ""
	}
	for p, data := range o.files {
		rest, ok := strings.CutPrefix(p, prefix)
		if !ok || rest == "" {
			continue
		}
		first, tail, nested := strings.Cut(rest, "/")
		if nested && tail != "" {
			byName[first] = fs.FileInfoToDirEntry(fileInfo{name: first, dir: true})
		} else {
			byName[first] = fs.FileInfoToDirEntry(fileInfo{name: first, size: int64(len(data))})
		}
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]fs.DirEntry, 0, len(names))
	for _, n := range names {
		out = append(out, byName[n])
	}
	return out, nil
}

func (o *overlay) Open(name string) (fs.File, error) { return openVia(o, name) }

// fileInfo is the metadata of a virtual entry.
type fileInfo struct {
	name string
	size int64
	dir  bool
}

func (i fileInfo) Name() string { return i.name }
func (i fileInfo) Size() int64  { return i.size }
func (i fileInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (i fileInfo) ModTime() time.Time { return time.Time{} }
func (i fileInfo) IsDir() bool        { return i.dir }
func (i fileInfo) Sys() any           { return nil }
