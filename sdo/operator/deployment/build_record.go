package deployment

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"sdo.dev/operator/safefs"
)

// BuildRecord remembers, per image, the effective tree of its last good
// turn-time build, so a build with no names rebuilds only what changed since.
type BuildRecord interface {
	LastGood(image string) (tree string, ok bool, err error)
	RecordGood(image, tree string) error
}

// BuildRecordFile is a BuildRecord kept as one JSON object (image to tree) in
// Rel below Root. It is the operate process's own state; a missing file is an
// empty record.
type BuildRecordFile struct {
	Root string
	Rel  string
}

var buildRecordMu sync.Mutex

func (f BuildRecordFile) read() (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join(f.Root, filepath.FromSlash(f.Rel)))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("image build record %s: %w", f.Rel, err)
	}
	return out, nil
}

// LastGood returns the tree of image's last good build.
func (f BuildRecordFile) LastGood(image string) (string, bool, error) {
	buildRecordMu.Lock()
	defer buildRecordMu.Unlock()
	records, err := f.read()
	if err != nil {
		return "", false, err
	}
	tree, ok := records[image]
	return tree, ok, nil
}

// RecordGood records that image built from tree.
func (f BuildRecordFile) RecordGood(image, tree string) error {
	buildRecordMu.Lock()
	defer buildRecordMu.Unlock()
	records, err := f.read()
	if err != nil {
		return err
	}
	records[image] = tree
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	if err := safefs.MkdirAll(f.Root, path.Dir(f.Rel), 0o700); err != nil {
		return err
	}
	return safefs.WriteFile(f.Root, f.Rel, data, 0o600)
}
