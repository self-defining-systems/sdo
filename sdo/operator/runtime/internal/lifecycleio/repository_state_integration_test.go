//go:build integration

package lifecycleio

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func TestRepositoryStateNamesHeadAndDirtySnapshot(t *testing.T) {
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "a.go"), []byte("package a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", "-A")
	runGit(t, dir, "commit", "-q", "-m", "one")

	clean, err := RepositoryState(context.Background(), dir)
	if err != nil || clean.Commit == "" || clean.Dirty || clean.Snapshot != "" {
		t.Fatalf("clean state = %+v, %v", clean, err)
	}
	// An untracked playbook is uncommitted work the snapshot must hold.
	if err := os.WriteFile(filepath.Join(dir, "new.go"), []byte("package a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirty, err := RepositoryState(context.Background(), dir)
	if err != nil || dirty.Commit != clean.Commit || !dirty.Dirty || dirty.Snapshot == "" || dirty.Snapshot == clean.Commit {
		t.Fatalf("dirty state = %+v, %v", dirty, err)
	}
	out, err := GitOutput(context.Background(), dir, "ls-tree", "--name-only", dirty.Snapshot)
	if err != nil || string(out) != "a.go\nnew.go\n" {
		t.Fatalf("snapshot tree = %q, %v", out, err)
	}
	// The checkout is untouched: still dirty, no new refs.
	if again, _ := RepositoryState(context.Background(), dir); again.Commit != clean.Commit || !again.Dirty {
		t.Fatalf("snapshot moved the checkout: %+v", again)
	}
}
