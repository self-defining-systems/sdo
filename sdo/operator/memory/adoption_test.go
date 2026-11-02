package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
)

func TestAdoptionPreservesMemoryButDiscardsInheritedLearning(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.name", "T")
	git("config", "user.email", "t@example.com")
	dir := filepath.Join(root, ".sdo/playbooks/repair")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	code := []byte("package repair\n")
	if err := os.WriteFile(filepath.Join(dir, "diagnose.go"), code, 0644); err != nil {
		t.Fatal(err)
	}
	store := NewJSONStore(root)
	old, err := store.Put(ctx, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inherited", Classification: "success"})
	if err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "seed")
	commit, err := MarkAdoptedExisting(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := AdoptionCommit(ctx, root, "HEAD"); err != nil || got != commit {
		t.Fatalf("adoption: %s %v", got, err)
	}
	if adopted, err := AdoptedBuild(ctx, root, "HEAD"); err != nil || !adopted {
		t.Fatalf("adopted build: %v %v", adopted, err)
	}
	if got := git("show", commit+":.sdo/playbooks/repair/diagnose.go"); got != strings.TrimSpace(string(code)) {
		t.Fatal("adoption changed program")
	}
	fresh, err := store.Put(ctx, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "new-live", Classification: "success"})
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := AfterAdoption(ctx, root, []Outcome{old, fresh})
	if err != nil || len(filtered) != 1 || filtered[0].IncidentID != "new-live" {
		t.Fatalf("inherited learning: %+v %v", filtered, err)
	}
	ignored := filepath.Join(root, ".sdo/lib/new/helper.go")
	if err := os.MkdirAll(filepath.Dir(ignored), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git/info/exclude"), []byte(".sdo/lib/\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ignored, []byte("package helper\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if adopted, err := AdoptedBuild(ctx, root, "HEAD"); err != nil || adopted {
		t.Fatalf("untracked ignored library inherited bypass: %v %v", adopted, err)
	}
	if err := os.RemoveAll(filepath.Join(root, ".sdo/lib")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "diagnose.go"), append(code, []byte("// later edit\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := MarkAdoptedExisting(ctx, root); err == nil {
		t.Fatal("second adoption bypassed later broker")
	}
	git("add", ".")
	git("commit", "-qm", "later accepted change")
	if adopted, err := AdoptedBuild(ctx, root, "HEAD"); err != nil || adopted {
		t.Fatalf("changed memory inherited bypass: %v %v", adopted, err)
	}
}
