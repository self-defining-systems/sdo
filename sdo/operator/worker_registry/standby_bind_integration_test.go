//go:build integration

package worker_registry

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
)

const bindReason = "The service must re-register with the registry after it restarts or it never receives traffic."

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

// patchedCheckout is what the controller hands a standby pod: a checkout of an
// accepted commit that carries a deployer patch series, with that series
// already materialized into the working files and its effective view recorded.
func patchedCheckout(t *testing.T) (checkout, base string) {
	t.Helper()
	ctx := context.Background()
	accepted := t.TempDir()
	testutil.InitGit(t, accepted)
	writeBindFile(t, accepted, "registry/registry.go", "a\nb\nc\n")
	writeBindFile(t, accepted, "main.go", "m\n")
	testutil.Git(t, accepted, "add", "-A")
	testutil.Git(t, accepted, "commit", "-qm", "base")
	// Author the patch the way a deployer does, in a materialized checkout.
	author := filepath.Join(t.TempDir(), "author")
	testutil.Git(t, accepted, "clone", "-q", accepted, author)
	testutil.ConfigureGit(t, author)
	if _, err := patchset.Materialize(ctx, author); err != nil {
		t.Fatal(err)
	}
	writeBindFile(t, author, "registry/registry.go", "a\nB\nc\n")
	if _, err := patchset.Create(ctx, author, patchset.CreateRequest{Owner: patchset.OwnerDeployer, Reason: bindReason}); err != nil {
		t.Fatal(err)
	}
	patches, err := os.ReadDir(filepath.Join(author, patchset.Directory))
	if err != nil || len(patches) != 1 {
		t.Fatalf("patches = %v, %v", patches, err)
	}
	data, err := os.ReadFile(filepath.Join(author, patchset.Directory, patches[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	writeBindFile(t, accepted, patchset.Directory+"/"+patches[0].Name(), string(data))
	testutil.Git(t, accepted, "add", "-A")
	testutil.Git(t, accepted, "commit", "-qm", "deployment patch")
	base = testutil.Git(t, accepted, "rev-parse", "HEAD")
	checkout = filepath.Join(t.TempDir(), "checkout")
	testutil.Git(t, accepted, "clone", "-q", accepted, checkout)
	if _, err := patchset.Materialize(ctx, checkout); err != nil {
		t.Fatal(err)
	}
	return checkout, base
}

// Binding a claimed standby to its accepted commit must leave the checkout the
// controller prepared as it is: the deployment patch series stays materialized,
// so the files the agent sees are the effective view the broker measures its
// scratch against. A hard reset here reverts them to the bare base, and the
// broker then reports every patched file as an uncommitted app edit.
func TestBindingAStandbyKeepsTheMaterializedView(t *testing.T) {
	ctx := context.Background()
	checkout, base := patchedCheckout(t)
	if err := bindAcceptedCommit(ctx, checkout, base); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(checkout, "registry/registry.go"))
	if err != nil || string(data) != "a\nB\nc\n" {
		t.Fatalf("registry/registry.go = %q, %v; the patched content must survive the bind", data, err)
	}
	scratch, err := patchset.Scratch(ctx, checkout)
	if err != nil || len(scratch) != 0 {
		t.Fatalf("scratch after bind = %v, %v; an untouched checkout has no uncommitted app files", scratch, err)
	}
}

// A real leftover edit made before the bind is neither hidden nor discarded.
func TestBindingAStandbyKeepsScratchEdits(t *testing.T) {
	ctx := context.Background()
	checkout, base := patchedCheckout(t)
	writeBindFile(t, checkout, "main.go", "edited\n")
	if err := bindAcceptedCommit(ctx, checkout, base); err != nil {
		t.Fatal(err)
	}
	scratch, err := patchset.Scratch(ctx, checkout)
	if err != nil || strings.Join(scratch, ",") != "main.go" {
		t.Fatalf("scratch = %v, %v; want only main.go", scratch, err)
	}
}

// A checkout that is not at the task's accepted commit is refused, not rewritten.
func TestBindingAStandbyRefusesAWrongCommit(t *testing.T) {
	checkout, base := patchedCheckout(t)
	other := strings.Repeat("0", len(base))
	if err := bindAcceptedCommit(context.Background(), checkout, other); err == nil {
		t.Fatal("bind to a commit the checkout is not at must fail")
	}
	if err := bindAcceptedCommit(context.Background(), checkout, ""); err == nil {
		t.Fatal("bind without an accepted commit must fail")
	}
}
