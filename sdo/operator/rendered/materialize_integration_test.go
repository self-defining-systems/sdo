//go:build integration

package rendered

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/patchset"
)

func TestMaterializeWritesTheEffectiveFilesOnly(t *testing.T) {
	dir, base := patchedRepo(t)
	write(t, dir, ".gitignore", "ignored.txt\n")
	write(t, dir, "ignored.txt", "scratch")
	write(t, dir, "bin/run.sh", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(dir, "bin/run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	base = commitTree(t, dir)
	eff := effective(t, ObjectStore{Dir: dir}, base)
	root := filepath.Join(t.TempDir(), "effective")
	m := &Materializer{Root: root}
	out, release, err := m.Materialize(context.Background(), eff)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if filepath.Dir(out) != root || filepath.Base(out) != eff.ID[:12] {
		t.Fatalf("directory = %s", out)
	}
	if got := mustRead(t, filepath.Join(out, "app/deploy.yaml")); got != readAll(t, eff, "app/deploy.yaml") || !contains(got, "image: patched") {
		t.Fatalf("patched file = %q", got)
	}
	for _, gone := range []string{"ignored.txt", ".sdo/playbooks", ".sdo/deployment/plan.yaml", patchset.Directory} {
		if _, err := os.Lstat(filepath.Join(out, gone)); err == nil {
			t.Errorf("%s must not be materialized", gone)
		}
	}
	if _, err := os.Stat(filepath.Join(out, ".sdo/deployment/values/v.yaml")); err != nil {
		t.Errorf("values must be materialized: %v", err)
	}
	if info, err := os.Stat(filepath.Join(out, "bin/run.sh")); err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Errorf("executable bit lost: %v %v", info, err)
	}
	// Same tree again: same directory, no hidden leftovers.
	again, release2, err := m.Materialize(context.Background(), eff)
	if err != nil || again != out {
		t.Fatalf("second materialize = %s, %v", again, err)
	}
	release2()
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Fatalf("leftovers under root: %v", entries)
	}
}

func TestMaterializePrunesSupersededUnreferencedTrees(t *testing.T) {
	dir, base := patchedRepo(t)
	store := ObjectStore{Dir: dir}
	root := filepath.Join(t.TempDir(), "effective")
	m := &Materializer{Root: root, Keep: 1, MinAge: time.Nanosecond}
	var dirs []string
	var releases []func()
	for i := 0; i < 3; i++ {
		write(t, dir, "app/n.txt", string(rune('a'+i)))
		eff := effective(t, store, commitTree(t, dir))
		out, release, err := m.Materialize(context.Background(), eff)
		if err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, out)
		releases = append(releases, release)
		time.Sleep(5 * time.Millisecond)
	}
	_ = base
	// All three are still held: nothing was pruned.
	for _, d := range dirs {
		if _, err := os.Stat(d); err != nil {
			t.Fatalf("held directory pruned: %v", err)
		}
	}
	for _, release := range releases {
		release()
	}
	old := time.Now().Add(-time.Hour)
	for _, d := range dirs {
		_ = os.Chtimes(d, old, old)
	}
	write(t, dir, "app/n.txt", "z")
	latest := effective(t, store, commitTree(t, dir))
	out, release, err := m.Materialize(context.Background(), latest)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, d := range dirs {
		if _, err := os.Stat(d); err == nil {
			t.Errorf("superseded unreferenced %s survived", d)
		}
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
