//go:build integration

package validate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitIn(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit initializes root as a repository, optionally adds app.txt, and commits
// everything, so later edits are a diff against "HEAD".
func commit(t *testing.T, root, message string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		gitIn(t, root, "init", "-q")
		gitIn(t, root, "config", "user.name", "Test")
		gitIn(t, root, "config", "user.email", "test@example.com")
	}
	if _, err := os.Stat(filepath.Join(root, "app.txt")); err != nil {
		write(t, root, "app.txt", "base\n")
	}
	gitIn(t, root, "add", "-A")
	gitIn(t, root, "commit", "-q", "--allow-empty", "-m", message)
}
