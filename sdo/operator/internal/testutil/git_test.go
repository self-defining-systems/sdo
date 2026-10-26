package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGitRepositoriesDisableBackgroundMaintenance(t *testing.T) {
	original := t.TempDir()
	InitGit(t, original)
	Git(t, original, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base")
	clone := filepath.Join(t.TempDir(), "clone")
	CloneGit(t, original, clone)
	worktree := filepath.Join(t.TempDir(), "worktree")
	Git(t, original, "worktree", "add", "--detach", "-q", worktree)
	for _, root := range []string{original, clone, worktree} {
		for key, want := range map[string]string{
			"gc.auto": "0", "maintenance.auto": "false",
			"gc.autoDetach": "false", "maintenance.autoDetach": "false",
		} {
			if got := Git(t, root, "config", "--get", key); got != want {
				t.Errorf("%s: %s = %q, want %q", root, key, got, want)
			}
		}
	}
}

func TestGitWaitsForCommandCompletion(t *testing.T) {
	root := t.TempDir()
	InitGit(t, root)
	Git(t, root, "-c", "alias.finish=!sleep 0.05; touch finished", "finish")
	if _, err := os.Stat(filepath.Join(root, "finished")); err != nil {
		t.Fatalf("Git returned before its command completed: %v", err)
	}
}
