// Package render defines the renderer seam: a pure function from one Git-tree
// view, a recorded environment and a typed config to Kubernetes objects.
//
// This package imports only the standard library, protobuf and apimachinery.
// Renderer implementations live in sub-packages and import this one.
package render

import (
	"crypto/sha1"
	"encoding/hex"
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
	// Blob returns the Git blob ID of the tracked file name. It does not read
	// the file's bytes into the renderer. A missing path is fs.ErrNotExist.
	//
	// There is deliberately no tree ID here: the tree ID covers .sdo/ memory,
	// so a renderer that saw it would let a memory-only commit change desired
	// state. Core keeps the tree beside the FS.
	Blob(name string) (string, error)
}

// GitBlobID is the Git blob object ID of data (sha1 of "blob <n>\0" + data).
func GitBlobID(data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// Limits bounds what one render may read.
type Limits struct {
	// FileBytes caps one file. TotalBytes caps the distinct files read.
	// OutputBytes caps the JSON-encoded size of every object a render
	// returns, ephemeral phases included. Zero fields take the defaults.
	FileBytes, TotalBytes, OutputBytes int64
}

// DefaultLimits are 2 MiB per file, 16 MiB read per render and 16 MiB
// rendered. The output bound equals the total input bound: a render may not
// emit more manifest bytes than the whole input budget it was allowed to read,
// which still leaves room for a template or generator to amplify a small chart
// by far more than any real application needs (the largest synthetic and real
// fixtures render to well under 1 MiB). Template expansion is the only
// amplification a renderer has, and this is the one place that caps it.
var DefaultLimits = Limits{FileBytes: 2 << 20, TotalBytes: 16 << 20, OutputBytes: 16 << 20}

func (l Limits) withDefaults() Limits {
	if l.FileBytes <= 0 {
		l.FileBytes = DefaultLimits.FileBytes
	}
	if l.TotalBytes <= 0 {
		l.TotalBytes = DefaultLimits.TotalBytes
	}
	if l.OutputBytes <= 0 {
		l.OutputBytes = DefaultLimits.OutputBytes
	}
	return l
}

// ErrLimit marks an input that exceeds Limits.
var ErrLimit = errors.New("render input exceeds limit")

// OutputLimitError reports a render whose objects, once encoded, exceed
// Limits.OutputBytes. It is a verdict on the source (Invalid), never retried.
// It matches ErrLimit through errors.Is.
type OutputLimitError struct {
	Bytes, Limit int64 // encoded size reached (a lower bound when stopped early) and the cap
	Objects      int   // objects counted when the cap was crossed
}

func (e *OutputLimitError) Error() string {
	return fmt.Sprintf("render output exceeds limit: %d bytes after %d objects, output limit %d", e.Bytes, e.Objects, e.Limit)
}

// Is makes the error match ErrLimit.
func (e *OutputLimitError) Is(target error) bool { return target == ErrLimit }

// InputKind says how a render touched an input.
type InputKind int

const (
	// InputRead: the file's bytes were read.
	InputRead InputKind = iota + 1
	// InputStat: the file was declared by a Stat without reading its bytes.
	// This is how SecretsOmit renders keep Secret-only inputs attributable.
	InputStat
	// InputListed: the directory's entries were listed.
	InputListed
	// InputAbsent: the path was probed and does not exist. Adding it later
	// changes the render.
	InputAbsent
)

func (k InputKind) String() string {
	switch k {
	case InputRead:
		return "read"
	case InputStat:
		return "stat"
	case InputListed:
		return "listed"
	case InputAbsent:
		return "absent"
	}
	return fmt.Sprintf("InputKind(%d)", int(k))
}

// Input is one recorded dependency of a render.
type Input struct {
	Path string // slash-separated, relative; "." is the root
	// Blob is the Git blob ID for Read and Stat of a file. It is empty for
	// directories and absent paths.
	Blob string
	Kind InputKind
}

// Recorder wraps an FS, enforces Limits and records every path read,
// declared, listed or probed. The catalog, never the renderer, turns the
// record into Result.Inputs.
type Recorder struct {
	fsys   FS
	limits Limits

	mu     sync.Mutex
	inputs map[string]Input
	sizes  map[string]int64 // files read: each distinct file counted once
	total  int64
}

var _ FS = (*Recorder)(nil)

// Record wraps fsys. Zero Limits fields take DefaultLimits.
func Record(fsys FS, limits Limits) *Recorder {
	return &Recorder{fsys: fsys, limits: limits.withDefaults(), inputs: map[string]Input{}, sizes: map[string]int64{}}
}

// Blob returns the wrapped blob ID without recording an input.
func (r *Recorder) Blob(name string) (string, error) { return r.fsys.Blob(name) }

// Inputs lists everything recorded, sorted by path. A path read after being
// declared is one Read input.
func (r *Recorder) Inputs() []Input {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Input, 0, len(r.inputs))
	for _, in := range r.inputs {
		out = append(out, in)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// note records in, keeping the strongest kind for a file (Read over Stat).
func (r *Recorder) note(in Input) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.inputs[in.Path]; ok && prev.Kind == InputRead && in.Kind == InputStat {
		return
	}
	r.inputs[in.Path] = in
}

func (r *Recorder) noteAbsent(name string, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		r.note(Input{Path: name, Kind: InputAbsent})
	}
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
		r.noteAbsent(name, err)
		return nil, err
	}
	if err := r.admit(name, int64(len(data))); err != nil {
		return nil, err
	}
	blob, err := r.fsys.Blob(name)
	if err != nil {
		return nil, err
	}
	r.note(Input{Path: name, Blob: blob, Kind: InputRead})
	return data, nil
}

// admit charges size against the limits. A file already charged with the
// same size is free.
func (r *Recorder) admit(name string, size int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if size > r.limits.FileBytes {
		return fmt.Errorf("%w: %s is %d bytes, file limit %d", ErrLimit, name, size, r.limits.FileBytes)
	}
	prev, seen := r.sizes[name]
	if seen && prev == size {
		return nil
	}
	if r.total-prev+size > r.limits.TotalBytes {
		return fmt.Errorf("%w: reading %s exceeds total limit %d", ErrLimit, name, r.limits.TotalBytes)
	}
	r.total += size - prev
	r.sizes[name] = size
	return nil
}

// ReadDir lists and records name.
func (r *Recorder) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	entries, err := r.fsys.ReadDir(name)
	if err != nil {
		r.noteAbsent(name, err)
		return nil, err
	}
	r.note(Input{Path: name, Kind: InputListed})
	return entries, nil
}

// Stat reports file metadata and records the path: as InputStat with its blob
// ID when it exists (its bytes are not read and not charged to the limits),
// as InputAbsent when it does not.
func (r *Recorder) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	info, err := r.fsys.Stat(name)
	if err != nil {
		r.noteAbsent(name, err)
		return nil, err
	}
	in := Input{Path: name, Kind: InputStat}
	if !info.IsDir() {
		if in.Blob, err = r.fsys.Blob(name); err != nil {
			return nil, err
		}
	}
	r.note(in)
	return info, nil
}

// Open opens name. Files are read whole through ReadFile (so they are
// recorded and limited); directories are listed through ReadDir.
func (r *Recorder) Open(name string) (fs.File, error) {
	return openVia(r, name)
}

// Overlay returns fsys with extra virtual files (chain hand-off, kustomize
// virtual Secret references). A virtual file shadows a real one. Reads of
// virtual files do not reach the wrapped FS, so a Recorder wrapped around
// fsys does not record them as inputs.
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

func (o *overlay) Blob(name string) (string, error) {
	if data, ok := o.files[name]; ok {
		return GitBlobID(data), nil
	}
	return o.FS.Blob(name)
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
