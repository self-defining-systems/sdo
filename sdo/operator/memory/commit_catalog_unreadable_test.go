package memory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
)

// A commit whose logical memory the reader rejects fails the same way every
// time, so SnapshotAt names it (ErrUnreadable) for callers that must tell it
// from a Git or IO failure that a retry can clear.
func TestSnapshotAtClassifiesUnreadableMemory(t *testing.T) {
	dir := appRepoWithout(t)
	version := filepath.Join(dir, filepath.FromSlash(layout.SchemaVersionFile))
	if err := os.WriteFile(version, []byte(strconv.Itoa(layout.CurrentSchemaVersion)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, dir, "add", "-A", "-f")
	testutil.Git(t, dir, "commit", "-qm", "schema")
	store := NewJSONStore(dir)
	good := strings.TrimSpace(testutil.Git(t, dir, "rev-parse", "HEAD"))
	if _, err := store.SnapshotAt(context.Background(), good); err != nil {
		t.Fatalf("a well-formed memory commit must read: %v", err)
	}
	stray := filepath.Join(dir, ".sdo", "playbooks", "README.md-1f6148")
	if err := os.MkdirAll(filepath.Dir(stray), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stray, []byte("synthetic stray file\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, dir, "add", "-A", "-f")
	testutil.Git(t, dir, "commit", "-qm", "stray")
	bad := strings.TrimSpace(testutil.Git(t, dir, "rev-parse", "HEAD"))
	_, err := store.SnapshotAt(context.Background(), bad)
	if !errors.Is(err, ErrUnreadable) {
		t.Fatalf("SnapshotAt over a stray file = %v; want ErrUnreadable", err)
	}
	if !strings.Contains(err.Error(), "README.md-1f6148") {
		t.Fatalf("error %q must name the offending file", err)
	}
	// A commit that is not accepted fails in Git, which a retry may clear: not unreadable.
	_, err = store.SnapshotAt(context.Background(), strings.Repeat("a", 40))
	if err == nil || errors.Is(err, ErrUnreadable) {
		t.Fatalf("SnapshotAt over an unknown commit = %v; want a non-ErrUnreadable error", err)
	}
}
