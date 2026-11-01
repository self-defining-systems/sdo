//go:build integration

package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostRequiresCleanAcceptedGitRoot(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if _, err := acceptedHead(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "uncommitted"), []byte("work"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := acceptedHead(context.Background(), root); err == nil || !strings.Contains(err.Error(), "uncommitted") {
		t.Fatalf("dirty source accepted: %v", err)
	}
}
