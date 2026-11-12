//go:build integration

package broker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/rendered"
)

const patchReason = "Pin the vendored library line the deployment depends on for this application"

// patchWorld is a followed source whose accepted tree carries a patch series
// over the vendored file lib/vendor.txt. Series authors work in a scratch
// clone so every patch is a real `git diff` against the tree it applies to.
type patchWorld struct {
	*advanceWorld
	scratch string
	next    int
}

func newPatchWorld(t *testing.T, editedByDeployer bool) *patchWorld {
	t.Helper()
	w := &patchWorld{advanceWorld: newOnboardedWorld(t, editedByDeployer)}
	w.scratch = filepath.Join(t.TempDir(), "author")
	testutil.Git(t, filepath.Dir(w.scratch), "clone", "-q", w.accepted, w.scratch)
	testutil.ConfigureGit(t, w.scratch)
	return w
}

// addPatch authors a patch by editing lib/vendor.txt in the scratch clone,
// keeps the edit there (so the next patch stacks on it) and commits the patch
// file into the accepted repository.
func (w *patchWorld) addPatch(t *testing.T, owner string, edit func(string) string) string {
	t.Helper()
	path := filepath.Join(w.scratch, "lib", "vendor.txt")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(edit(string(before))), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, w.scratch, "add", "lib/vendor.txt")
	diff := testutil.Git(t, w.scratch, "diff", "--cached", "--no-color", "--full-index", "--no-ext-diff")
	testutil.Git(t, w.scratch, "-c", "user.name=t", "-c", "user.email=t@example.test", "commit", "-qm", "step")
	w.next++
	name := "000" + string(rune('0'+w.next)) + "-vendor.patch"
	incident := ""
	if owner == "responder" {
		incident = "Incident: inc-1\n"
	}
	body := "SDO-Patch: 1\nOwner: " + owner + "\n" + incident + "Reason: |\n  " + patchReason + "\n\n" + diff + "\n"
	writeFiles(t, w.accepted, map[string]string{".sdo/deployment/patches/" + name: body})
	testutil.Git(t, w.accepted, "add", ".")
	testutil.Git(t, w.accepted, "commit", "-qm", "test setup: "+name)
	return name
}

func replaceLine(from, to string) func(string) string {
	return func(text string) string { return strings.Replace(text, from+"\n", to+"\n", 1) }
}

func effectiveFile(t *testing.T, repository, tree, path string) string {
	t.Helper()
	eff, err := rendered.Effective(context.Background(), rendered.ObjectStore{Dir: repository}, rendered.BaseTree(tree))
	if err != nil {
		t.Fatal(err)
	}
	defer eff.Close()
	cmd := exec.Command("git", "-C", repository, "cat-file", "-p", eff.ID+":"+path)
	cmd.Env = append(os.Environ(), eff.Store.Env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("effective %s: %v\n%s", path, err, out)
	}
	return string(out)
}
