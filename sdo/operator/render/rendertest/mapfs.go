// Package rendertest provides fixtures and the conformance suite for
// renderers, in the manner of testing/fstest.
package rendertest

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"sort"
	"testing/fstest"
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

// Tree is a deterministic digest of the sorted paths and contents.
func (m MapFS) Tree() string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write([]byte(m[name]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
