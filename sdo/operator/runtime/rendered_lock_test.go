package runtime

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"

	"sdo.dev/operator/rendered"
)

// Test fixtures publish a controller lock as a commit after the source they render.
func seedRenderedLock(t *testing.T, source GitPlanSource, revision, namespace string) string {
	t.Helper()
	snapshot, err := source.BuildLock(context.Background(), revision, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Validate(context.Background(), source.Repository, revision); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Write(source.Repository); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".sdo/rendered")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic controller lock")
	return nativeGit(t, source.Repository, "rev-parse", "HEAD")
}

// lockCurrent reports whether the lock committed at head was rendered from
// inputs equal to head's.
func lockCurrent(ctx context.Context, source GitPlanSource, head string) bool {
	out, err := exec.Command("git", "-C", source.Repository, "show", head+":.sdo/rendered/lock.json").Output()
	if err != nil {
		return false
	}
	var lock rendered.Lock
	if json.Unmarshal(out, &lock) != nil {
		return false
	}
	same, err := source.SameInputs(ctx, lock.SourceCommit, head)
	return err == nil && same
}
