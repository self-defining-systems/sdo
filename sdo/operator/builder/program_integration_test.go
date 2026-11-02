//go:build integration

package builder

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"sdo.dev/operator/internal/testutil"
	programvalidation "sdo.dev/operator/memory/validate/program"
)

// Declaration rules judge only the playbooks a candidate edits, so accepted
// memory never blocks an outcome publication that does not touch it.
func TestChangedProgramsAreTheCandidateEditedPlaybooks(t *testing.T) {
	root := t.TempDir()
	programs := []programvalidation.Spec{{ID: "edited"}, {ID: "accepted"}, {ID: "added"}}
	if got := changedPrograms(context.Background(), root, programs); !slices.Equal(got, []string{"edited", "accepted", "added"}) {
		t.Fatalf("without Git every program is checked: %v", got)
	}
	git := func(args ...string) {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", root, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, output)
		}
	}
	write := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q")
	write(".sdo/playbooks/edited/diagnose.go", "package edited\n")
	write(".sdo/playbooks/accepted/diagnose.go", "package accepted\n")
	git("add", ".")
	git("commit", "-q", "-m", "accepted")
	if got := changedPrograms(context.Background(), root, programs); len(got) != 0 {
		t.Fatalf("accepted programs re-judged: %v", got)
	}
	write(".sdo/playbooks/edited/README.md", "# edited\n")
	write(".sdo/playbooks/added/diagnose.go", "package added\n")
	write(".sdo/outcomes/x.json", "{}")
	if got := changedPrograms(context.Background(), root, programs); !slices.Equal(got, []string{"edited", "added"}) {
		t.Fatalf("edited playbooks: %v", got)
	}
}
