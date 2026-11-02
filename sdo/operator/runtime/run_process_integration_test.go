//go:build integration

package runtime

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestResolveRepositoryCommitsUsesRealGitHead(t *testing.T) {
	repository := t.TempDir()
	testutil.InitGit(t, repository)
	commands := [][]string{
		{"git", "-C", repository, "config", "user.name", "test"},
		{"git", "-C", repository, "config", "user.email", "test@example.com"},
	}
	for _, command := range commands {
		if output, err := exec.Command(command[0], command[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("run %v: %v: %s", command, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repository, "app.txt"), []byte("app\n"), 0o644); err != nil {
		t.Fatalf("write app: %v", err)
	}
	for _, args := range [][]string{{"add", "app.txt"}, {"commit", "-qm", "initial"}} {
		if output, err := exec.Command("git", append([]string{"-C", repository}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	wantBytes, err := exec.Command("git", "-C", repository, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("read head: %v", err)
	}
	want := string(bytes.TrimSpace(wantBytes))

	source, deployed, err := resolveRepositoryCommits(repository, "", "")
	if err != nil {
		t.Fatalf("resolve commits: %v", err)
	}
	if source != want || deployed != want {
		t.Fatalf("unexpected commits source=%q deployed=%q want=%q", source, deployed, want)
	}
}
