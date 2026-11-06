//go:build integration

package gitpoll

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"sdo.dev/operator/upstream"
	"sdo.dev/operator/upstream/upstreamtest"
)

// The remote is unauthenticated, so a revision whose objects fail git's
// integrity check (here a commit with no author) must not enter the accepted
// object store, while a well-formed one still does.
func TestMaterializeRejectsCorruptObjects(t *testing.T) {
	r := upstreamtest.NewRemote(t)
	hash := func(kind, content string) string {
		t.Helper()
		cmd := exec.Command("git", "--git-dir", r.URL, "hash-object", "-w", "--literally", "-t", kind, "--stdin")
		cmd.Stdin = strings.NewReader(content)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("hash-object: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	tree := hash("tree", "")
	bad := hash("commit", "tree "+tree+"\ncommitter T <t@example.com> 0 +0000\n\nno author\n")
	store := upstreamtest.NewObjectStore(t)
	_, err := catalog(t).Materialize(context.Background(), ID, config(r.URL, "main"), upstream.Revision{ID: bad, Ref: "main"}, store)
	if err == nil {
		t.Fatal("a commit failing git's integrity check was materialized")
	}
	if out, gerr := exec.Command("git", "--git-dir", store, "cat-file", "-e", bad).CombinedOutput(); gerr == nil {
		t.Fatalf("corrupt object %s reached the store: %s", bad, out)
	}
	good := r.Push(t)
	if _, err := catalog(t).Materialize(context.Background(), ID, config(r.URL, "main"), upstream.Revision{ID: good, Ref: "main"}, store); err != nil {
		t.Fatalf("well-formed revision: %v", err)
	}
}
