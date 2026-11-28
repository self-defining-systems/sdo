package render

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Root is one place in a tree a renderer deploys from: its directory ("." for
// the tree root) and the config that renders it.
type Root struct {
	Dir    string
	Config proto.Message
}

// Detector finds a renderer's roots in a tree from the files present.
type Detector func(FS) ([]Root, error)

// WithDetect registers how the renderer recognizes its roots, so a repository
// that names no source is rendered from its files. A fallback detector is
// asked only when no other renderer finds a root.
func WithDetect(detect Detector, fallback bool) Option {
	return func(r *registration) { r.detect, r.fallback = detect, fallback }
}

// Detected is one root a registered renderer found.
type Detected struct {
	RendererID string
	Dir        string
	Config     *anypb.Any
	// Fallback marks a root found by a fallback detector.
	Fallback bool
}

// Detect asks every renderer with a detector for its roots, in renderer ID
// order. A root's config must be the renderer's own config type.
func (c *Catalog) Detect(fsys FS) ([]Detected, error) {
	var out []Detected
	for _, id := range c.IDs() {
		e := c.entries[id]
		if e.detect == nil {
			continue
		}
		roots, err := e.detect(fsys)
		if err != nil {
			return nil, fmt.Errorf("renderer %q: detect roots: %w", id, err)
		}
		for _, root := range roots {
			config, err := anypb.New(root.Config)
			if err != nil {
				return nil, fmt.Errorf("renderer %q: encode root %s: %w", id, root.Dir, err)
			}
			if config.GetTypeUrl() != e.typeURL {
				return nil, fmt.Errorf("renderer %q detected a root with config %s, want %s", id, config.GetTypeUrl(), e.typeURL)
			}
			out = append(out, Detected{RendererID: id, Dir: path.Clean(root.Dir), Config: config, Fallback: e.fallback})
		}
	}
	return out, nil
}

// TreeFiles lists the tree's files in sorted order, without hidden directories
// (.sdo/, .git/, .github/ and the like hold no application deployment).
func TreeFiles(fsys fs.FS) ([]string, error) {
	var files []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name != "." && strings.HasPrefix(path.Base(name), ".") {
				return fs.SkipDir
			}
			return nil
		}
		files = append(files, name)
		return nil
	})
	slices.Sort(files)
	return files, err
}

// Within reports whether dir is strictly below ancestor ("." is the root).
func Within(dir, ancestor string) bool {
	if dir == ancestor {
		return false
	}
	return ancestor == "." || strings.HasPrefix(dir, ancestor+"/")
}
