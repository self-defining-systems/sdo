//go:build integration

package worker_registry

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func writeBindFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// acceptedCheckout is what the controller hands a standby pod: a checkout of
// an accepted commit.
func acceptedCheckout(t *testing.T) (checkout, base string) {
	t.Helper()
	accepted := t.TempDir()
	testutil.InitGit(t, accepted)
	writeBindFile(t, accepted, "registry/registry.go", "a\nb\nc\n")
	writeBindFile(t, accepted, "main.go", "m\n")
	testutil.Git(t, accepted, "add", "-A")
	testutil.Git(t, accepted, "commit", "-qm", "base")
	base = strings.TrimSpace(testutil.Git(t, accepted, "rev-parse", "HEAD"))
	checkout = filepath.Join(t.TempDir(), "checkout")
	testutil.Git(t, accepted, "clone", "-q", accepted, checkout)
	return checkout, base
}

// A real leftover edit made before the bind is neither hidden nor discarded.
func TestBindingAStandbyKeepsScratchEdits(t *testing.T) {
	ctx := context.Background()
	checkout, base := acceptedCheckout(t)
	writeBindFile(t, checkout, "main.go", "edited\n")
	if err := bindAcceptedCommit(ctx, checkout, base); err != nil {
		t.Fatal(err)
	}
	if status := strings.TrimSpace(testutil.Git(t, checkout, "status", "--porcelain")); status != "M main.go" {
		t.Fatalf("status = %q; want only main.go modified", status)
	}
}

// A checkout that is not at the task's accepted commit is refused, not rewritten.
func TestBindingAStandbyRefusesAWrongCommit(t *testing.T) {
	checkout, base := acceptedCheckout(t)
	other := strings.Repeat("0", len(base))
	if err := bindAcceptedCommit(context.Background(), checkout, other); err == nil {
		t.Fatal("bind to a commit the checkout is not at must fail")
	}
	if err := bindAcceptedCommit(context.Background(), checkout, ""); err == nil {
		t.Fatal("bind without an accepted commit must fail")
	}
}
