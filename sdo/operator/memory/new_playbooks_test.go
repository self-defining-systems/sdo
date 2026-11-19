//go:build integration

package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

func gitIn(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func writeIn(t *testing.T, root, name string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A playbook is new when the base holds none of its program source. A playbook
// edited since, or retired with only a README, is not new, and a README alone
// is not a program.
func TestNewPlaybooksAreTheOnesWithoutProgramSourceAtTheBase(t *testing.T) {
	root := t.TempDir()
	gitIn(t, root, "init", "-q")
	writeIn(t, root, ".sdo/playbooks/old/diagnose.go")
	writeIn(t, root, ".sdo/playbooks/retired/README.md")
	gitIn(t, root, "add", "-A")
	gitIn(t, root, "commit", "-q", "-m", "base")
	writeIn(t, root, ".sdo/playbooks/old/mitigate.go")
	writeIn(t, root, ".sdo/playbooks/fresh/diagnose.go")
	writeIn(t, root, ".sdo/playbooks/fresh/diagnose_test.go")
	writeIn(t, root, ".sdo/playbooks/readme-only/README.md")

	got, err := NewPlaybooks(context.Background(), root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"fresh"}) {
		t.Fatalf("new playbooks = %v, want [fresh]", got)
	}
	bare := t.TempDir()
	writeIn(t, bare, ".sdo/playbooks/fresh/diagnose.go")
	if _, err := NewPlaybooks(context.Background(), bare, "HEAD"); err == nil {
		t.Fatal("a directory with no history cannot say what is new")
	}
}
