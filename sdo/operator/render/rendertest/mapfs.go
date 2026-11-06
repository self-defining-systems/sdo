// Package rendertest provides fixtures and the conformance suite for
// renderers, in the manner of testing/fstest.
package rendertest

import (
	"io/fs"
	"testing/fstest"

	"sdo.dev/operator/render"
)

// MapFS is an in-memory render.FS for fixtures: path -> content.
type MapFS map[string]string

func (m MapFS) fsys() fstest.MapFS {
	out := fstest.MapFS{}
	for name, content := range m {
		out[name] = &fstest.MapFile{Data: []byte(content), Mode: 0o644}
	}
	return out
}

// Open implements fs.FS.
func (m MapFS) Open(name string) (fs.File, error) { return m.fsys().Open(name) }

// ReadFile implements fs.ReadFileFS.
func (m MapFS) ReadFile(name string) ([]byte, error) { return m.fsys().ReadFile(name) }

// ReadDir implements fs.ReadDirFS.
func (m MapFS) ReadDir(name string) ([]fs.DirEntry, error) { return m.fsys().ReadDir(name) }

// Stat implements fs.StatFS.
func (m MapFS) Stat(name string) (fs.FileInfo, error) { return m.fsys().Stat(name) }

// Blob returns the Git blob ID of the file's content.
func (m MapFS) Blob(name string) (string, error) {
	content, ok := m[name]
	if !ok {
		return "", &fs.PathError{Op: "blob", Path: name, Err: fs.ErrNotExist}
	}
	return render.GitBlobID([]byte(content)), nil
}
