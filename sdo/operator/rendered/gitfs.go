package rendered

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/render"
)

// treeFS is the render.FS over one Git tree. The listing (paths, blob IDs,
// sizes) is loaded once, so Stat and Blob never read bytes.
type treeFS struct {
	ctx     context.Context
	objects ObjectStore
	files   map[string]treeFile
	dirs    map[string][]string // directory -> sorted child names
}

type treeFile struct {
	blob string
	size int64
	mode fs.FileMode
}

// GitTreeFS reads one effective tree through `git ls-tree -r -z --full-tree -l`
// and `git cat-file`. Modes 100644 and 100755 are files; symlinks (120000) and
// gitlinks (160000) do not exist in the view. Everything under .sdo/ is hidden
// except .sdo/deployment/, so memory-only commits cannot change a render.
// The context bounds later ReadFile calls too.
func GitTreeFS(ctx context.Context, objects ObjectStore, tree EffectiveTree) (render.FS, error) {
	id := string(tree)
	if !treeID.MatchString(id) {
		return nil, fmt.Errorf("effective tree %q is not a Git object ID", id)
	}
	if err := requireTree(ctx, objects, id); err != nil {
		return nil, err
	}
	out, err := objects.git(ctx, "ls-tree", "-r", "-z", "--full-tree", "-l", id)
	if err != nil {
		return nil, sourceErrorf(SourceUnavailable, "list %s in %s: %w", id, objects.Dir, err)
	}
	t := &treeFS{ctx: ctx, objects: objects, files: map[string]treeFile{}, dirs: map[string][]string{}}
	for _, record := range bytes.Split(out, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		meta, name, ok := strings.Cut(string(record), "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 4 {
			return nil, sourceErrorf(SourceUnavailable, "malformed ls-tree record %q", record)
		}
		var mode fs.FileMode
		switch fields[0] {
		case "100644":
			mode = 0o644
		case "100755":
			mode = 0o755
		default:
			continue
		}
		if hidden(name) {
			continue
		}
		size, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return nil, sourceErrorf(SourceUnavailable, "malformed size in ls-tree record %q", record)
		}
		t.files[name] = treeFile{blob: fields[2], size: size, mode: mode}
	}
	t.index()
	return t, nil
}

func hidden(name string) bool {
	return (name == ".sdo" || strings.HasPrefix(name, ".sdo/")) && !strings.HasPrefix(name, ".sdo/deployment/")
}

func (t *treeFS) index() {
	seen := map[string]bool{}
	add := func(dir, child string) {
		key := dir + "\x00" + child
		if !seen[key] {
			seen[key] = true
			t.dirs[dir] = append(t.dirs[dir], child)
		}
	}
	t.dirs["."] = nil
	for name := range t.files {
		for p := name; p != "."; {
			parent := path.Dir(p)
			add(parent, path.Base(p))
			p = parent
		}
	}
	for _, children := range t.dirs {
		sort.Strings(children)
	}
}

func (t *treeFS) isDir(name string) bool {
	_, ok := t.dirs[name]
	return ok
}

func notExist(op, name string) error { return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist} }

// Blob returns the Git blob ID of the tracked file without reading it.
func (t *treeFS) Blob(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "blob", Path: name, Err: fs.ErrInvalid}
	}
	f, ok := t.files[name]
	if !ok {
		return "", notExist("blob", name)
	}
	return f.blob, nil
}

// ReadFile returns the blob's bytes via git cat-file.
func (t *treeFS) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrInvalid}
	}
	f, ok := t.files[name]
	if !ok {
		if t.isDir(name) {
			return nil, &fs.PathError{Op: "read", Path: name, Err: errors.New("is a directory")}
		}
		return nil, notExist("read", name)
	}
	data, err := t.objects.git(t.ctx, "cat-file", "blob", f.blob)
	if err != nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: sourceErrorf(SourceUnavailable, "blob %s: %w", f.blob, err)}
	}
	return data, nil
}

// Stat describes a file or directory from the loaded listing.
func (t *treeFS) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	if f, ok := t.files[name]; ok {
		return fileInfo{name: path.Base(name), size: f.size, mode: f.mode}, nil
	}
	if t.isDir(name) {
		return fileInfo{name: path.Base(name), mode: fs.ModeDir | 0o755}, nil
	}
	return nil, notExist("stat", name)
}

// ReadDir lists a directory sorted by name.
func (t *treeFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	children, ok := t.dirs[name]
	if !ok {
		if _, file := t.files[name]; file {
			return nil, &fs.PathError{Op: "readdir", Path: name, Err: errors.New("not a directory")}
		}
		return nil, notExist("readdir", name)
	}
	entries := make([]fs.DirEntry, 0, len(children))
	for _, child := range children {
		info, err := t.Stat(path.Join(name, child))
		if err != nil {
			return nil, err
		}
		entries = append(entries, fs.FileInfoToDirEntry(info))
	}
	return entries, nil
}

// Open implements fs.FS over the same view.
func (t *treeFS) Open(name string) (fs.File, error) {
	info, err := t.Stat(name)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		entries, err := t.ReadDir(name)
		if err != nil {
			return nil, err
		}
		return &openDir{info: info, entries: entries}, nil
	}
	data, err := t.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return &openFile{Reader: bytes.NewReader(data), info: info}, nil
}

type fileInfo struct {
	name string
	size int64
	mode fs.FileMode
}

func (i fileInfo) Name() string       { return i.name }
func (i fileInfo) Size() int64        { return i.size }
func (i fileInfo) Mode() fs.FileMode  { return i.mode }
func (i fileInfo) ModTime() time.Time { return time.Time{} }
func (i fileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i fileInfo) Sys() any           { return nil }

type openFile struct {
	*bytes.Reader
	info fs.FileInfo
}

func (f *openFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *openFile) Close() error               { return nil }

type openDir struct {
	info    fs.FileInfo
	entries []fs.DirEntry
}

func (d *openDir) Stat() (fs.FileInfo, error) { return d.info, nil }
func (d *openDir) Close() error               { return nil }
func (d *openDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.Name(), Err: errors.New("is a directory")}
}

func (d *openDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		out := d.entries
		d.entries = nil
		return out, nil
	}
	if len(d.entries) == 0 {
		return nil, io.EOF
	}
	n = min(n, len(d.entries))
	out := d.entries[:n]
	d.entries = d.entries[n:]
	return out, nil
}
