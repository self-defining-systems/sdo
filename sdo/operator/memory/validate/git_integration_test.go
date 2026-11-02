package validate

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func gitIn(t *testing.T, root string, args ...string) string {
	t.Helper()
	return testutil.Git(t, root, args...)
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
