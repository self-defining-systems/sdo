package install

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The synced archive overlays the accepted tree onto the host checkout, so a file an accepted commit deleted
// (for example the retired .sdo/outcomes.jsonl) would otherwise linger as an untracked change and make the
// next install-existing refuse the checkout as dirty.
func TestRemoveStaleUntrackedDropsFilesTheAcceptedTreeDeleted(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid"}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	git("init", "-q")
	stale := filepath.Join(repo, ".sdo", "outcomes.jsonl")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "seed")
	git("rm", "-q", "--cached", ".sdo/outcomes.jsonl")
	git("commit", "-qm", "accepted commit deletes the legacy stream")
	if err := removeStaleUntracked(context.Background(), repo); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file survived synchronization: %v", err)
	}
	status := exec.Command("git", "-C", repo, "status", "--porcelain")
	if output, err := status.CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("synchronized checkout is dirty: %v: %s", err, output)
	}
}
