//go:build integration

package validate

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
)

// A materialized checkout holds the patch series in its app files. ChangedPaths
// must report those files as unchanged and only scratch edits (and .sdo/ edits)
// as changes, while an unmaterialized checkout keeps the plain diff.
func TestChangedPathsMeasuresAppFilesAgainstTheEffectiveView(t *testing.T) {
	ctx := context.Background()
	write := func(root, name, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	accepted := t.TempDir()
	testutil.InitGit(t, accepted)
	write(accepted, "app.txt", "one\ntwo\nthree\n")
	testutil.Git(t, accepted, "add", ".")
	testutil.Git(t, accepted, "commit", "-qm", "seed")
	// Author a patch with a real diff, then commit it.
	write(accepted, "app.txt", "one\ntwo\nthree patched\n")
	testutil.Git(t, accepted, "add", ".")
	diff := testutil.Git(t, accepted, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff")
	testutil.Git(t, accepted, "reset", "-q", "--hard", "HEAD")
	write(accepted, ".sdo/deployment/patches/0001-app.patch", "SDO-Patch: 1\nOwner: deployer\nReason: |\n  The application does not start without the third line being patched for the registry.\n\n"+diff+"\n")
	testutil.Git(t, accepted, "add", ".")
	testutil.Git(t, accepted, "commit", "-qm", "patch")
	base := testutil.Git(t, accepted, "rev-parse", "HEAD")

	plain := filepath.Join(t.TempDir(), "plain")
	testutil.Git(t, filepath.Dir(plain), "clone", "-q", accepted, plain)
	write(plain, "app.txt", "one\ntwo\nthree edited\n")
	if got, err := ChangedPaths(ctx, plain, base); err != nil || !reflect.DeepEqual(got, []string{"app.txt"}) {
		t.Fatalf("unmaterialized = %v, %v", got, err)
	}

	work := filepath.Join(t.TempDir(), "work")
	testutil.Git(t, filepath.Dir(work), "clone", "-q", accepted, work)
	if _, err := patchset.Materialize(ctx, work); err != nil {
		t.Fatal(err)
	}
	if got, err := ChangedPaths(ctx, work, base); err != nil || len(got) != 0 {
		t.Fatalf("materialized checkout reports %v, %v; the patched file is not a change", got, err)
	}
	write(work, "extra.txt", "x\n")
	write(work, ".sdo/playbooks/p.md", "note\n")
	want := []string{".sdo/playbooks/p.md", "extra.txt"}
	if got, err := ChangedPaths(ctx, work, base); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("materialized with edits = %v, %v; want %v", got, err, want)
	}
}
