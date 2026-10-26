package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func catalogGit(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = root
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, raw)
	}
	return strings.TrimSpace(string(raw))
}

func TestCatalogSnapshotAtRetainsProposalCommitAfterPeerPublication(t *testing.T) {
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS("validate/testdata/good")); err != nil {
		t.Fatal(err)
	}
	catalogGit(t, root, "init", "-q")
	catalogGit(t, root, "config", "user.name", "Test")
	catalogGit(t, root, "config", "user.email", "test@example.invalid")
	catalogGit(t, root, "add", ".")
	catalogGit(t, root, "commit", "-qm", "accepted seed")
	before := catalogGit(t, root, "rev-parse", "HEAD")
	reader := OpenCatalogReader(root)
	original, err := reader.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	goal := filepath.Join(root, ".sdo/goal.md")
	data, err := os.ReadFile(goal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goal, append(data, []byte("\nPeer accepted goal\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	// A peer changes checkout conversion policy as well as memory. Historical
	// catalog bytes must not use attributes from the newer mutable checkout.
	if err := os.WriteFile(filepath.Join(root, ".gitattributes"), []byte("*.md text eol=crlf\n"), 0644); err != nil {
		t.Fatal(err)
	}
	catalogGit(t, root, "add", ".")
	catalogGit(t, root, "commit", "-qm", "peer memory publication")
	after := catalogGit(t, root, "rev-parse", "HEAD")
	pinned, err := SnapshotAt(context.Background(), reader, before)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.GetAcceptedCommit() != before || pinned.GetGoalMarkdown() != original.GetGoalMarkdown() {
		t.Fatal("proposal memory silently advanced")
	}
	latest, err := SnapshotAt(context.Background(), reader, after)
	if err != nil || !strings.Contains(latest.GetGoalMarkdown(), "Peer accepted goal") {
		t.Fatalf("new proposal missed peer learning: %v", err)
	}
	// Reading immutable objects must neither trust nor modify the accepted index.
	if err := os.WriteFile(goal, []byte("unaccepted"), 0644); err != nil {
		t.Fatal(err)
	}
	pinned, err = SnapshotAt(context.Background(), reader, before)
	if err != nil || pinned.GetGoalMarkdown() != original.GetGoalMarkdown() {
		t.Fatalf("working tree leaked into pinned read: %v", err)
	}
	if got := catalogGit(t, root, "show", ":.sdo/goal.md"); !strings.Contains(got, "Peer accepted goal") {
		t.Fatal("pinned read changed accepted index")
	}
	for _, invalid := range []string{"HEAD", before[:12], strings.Repeat("0", 40)} {
		if _, err := SnapshotAt(context.Background(), reader, invalid); err == nil {
			t.Fatalf("unbound revision accepted: %q", invalid)
		}
	}
}
