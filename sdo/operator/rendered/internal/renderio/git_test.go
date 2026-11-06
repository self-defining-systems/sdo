package renderio

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

// Projecting an older revision of a repository whose working tree and HEAD
// have moved on must drop memory paths from the scratch index without git's
// "staged content differs from file and HEAD" refusal.
func TestProjectDropsPathsThatChangedSinceTheProjectedTree(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	write := func(rel, content string) {
		t.Helper()
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("app.txt", "source\n")
	write(".sdo/note.md", "one\n")
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "judged")
	judged := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD^{tree}"))
	write(".sdo/note.md", "two\n")
	testutil.Git(t, root, "commit", "-qam", "memory moved on")

	tree, err := Project(context.Background(), root, nil, judged, []string{".sdo"})
	if err != nil {
		t.Fatal(err)
	}
	listing := testutil.Git(t, root, "ls-tree", "-r", "--name-only", tree)
	if strings.TrimSpace(listing) != "app.txt" {
		t.Fatalf("projected tree holds %q, want only app.txt", listing)
	}
}
