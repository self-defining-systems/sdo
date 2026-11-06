package rendered

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"sdo.dev/operator/rendered/internal/renderio"
)

// Materializer writes effective trees to directories a build can mount: the
// content-addressed directory <Root>/<first 12 hex of the tree ID>, written
// once through a hidden temporary directory and renamed, so a reader never
// sees a half-written tree. Contents come from `git archive` of the effective
// tree, so a build sees exactly the tracked effective files (no ignored files,
// no scratch edits, no .sdo/ memory).
//
// A directory is pruned when no Materialize call in this process holds it,
// it is not among the Keep most recently used, and it was last used more than
// MinAge ago; the age guard also protects directories another process is
// still building from.
type Materializer struct {
	Root   string
	Keep   int           // default 4
	MinAge time.Duration // default 1h; negative means no age guard

	mu   sync.Mutex
	held map[string]int
}

const (
	defaultMaterializedKeep   = 4
	defaultMaterializedMinAge = time.Hour
)

// Materialize returns the directory holding tree's files and a release that
// must be called when the build finishes (the directory is then prunable).
// The tree's Store must still be open.
func (m *Materializer) Materialize(ctx context.Context, tree EffectiveTree) (string, func(), error) {
	if m == nil || m.Root == "" {
		return "", nil, errors.New("materializer needs a root directory")
	}
	if !treeID.MatchString(tree.ID) {
		return "", nil, fmt.Errorf("effective tree %q is not a Git object ID", tree.ID)
	}
	name := tree.ID[:12]
	dir := filepath.Join(m.Root, name)
	m.mu.Lock()
	if m.held == nil {
		m.held = map[string]int{}
	}
	m.held[name]++
	m.mu.Unlock()
	release := func() {
		m.mu.Lock()
		if m.held[name]--; m.held[name] <= 0 {
			delete(m.held, name)
		}
		m.mu.Unlock()
	}
	if err := m.ensure(ctx, tree, dir); err != nil {
		release()
		return "", nil, err
	}
	m.prune()
	return dir, release, nil
}

func (m *Materializer) ensure(ctx context.Context, tree EffectiveTree, dir string) error {
	if renderio.Exists(dir) {
		return nil
	}
	if err := os.MkdirAll(m.Root, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(m.Root, ".extract-")
	if err != nil {
		return err
	}
	staging := filepath.Join(tmp, "tree")
	if err := renderio.Extract(ctx, tree.Store.Dir, tree.Store.Env, tree.ID, staging); err != nil {
		_ = os.RemoveAll(tmp)
		return fmt.Errorf("materialize effective tree %s: %w", tree.ID, err)
	}
	if _, err := renderio.Publish(staging, dir); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	return os.RemoveAll(tmp)
}

func (m *Materializer) prune() {
	keep, age := m.Keep, m.MinAge
	if keep <= 0 {
		keep = defaultMaterializedKeep
	}
	if age == 0 {
		age = defaultMaterializedMinAge
	} else if age < 0 {
		age = 0
	}
	m.mu.Lock()
	held := make(map[string]bool, len(m.held))
	for name := range m.held {
		held[name] = true
	}
	m.mu.Unlock()
	// Pruning is housekeeping: a failure leaves directories for the next call.
	_ = renderio.Prune(m.Root, held, keep, age)
}
