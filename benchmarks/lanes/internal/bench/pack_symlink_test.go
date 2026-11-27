//go:build integration

package bench

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A pack variant whose files are symlinks to the pack's own (the GitLab release variants) is the pack as committed:
// git stores a symlink as a blob of its target path, so the working copy must be read the same way.
func TestPackCheckReadsSymlinkedFilesAsGitStoresThem(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@invalid")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("git", "init", "-q")
	variant := filepath.Join(dir, "examples", "app", "releases", "1.0")
	if err := os.MkdirAll(variant, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "examples", "app", "app.yaml"), []byte("name: app\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(variant, "pins.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../app.yaml", filepath.Join(variant, "app.yaml")); err != nil {
		t.Fatal(err)
	}
	run("git", "add", "-A")
	run("git", "commit", "-q", "-m", "pack")
	rev := run("git", "rev-parse", "HEAD")
	out, err := exec.Command("sh", "-c", packScript(dir, rev, "examples/app/releases/1.0")).CombinedOutput()
	if err != nil {
		t.Fatalf("pack script: %v\n%s", err, out)
	}
	if ok, detail := comparePack(string(out), rev); !ok {
		t.Fatalf("an unchanged symlinked pack differs: %s\n%s", detail, out)
	}
	// a symlink pointed elsewhere is a modified pack file
	if err := os.Remove(filepath.Join(variant, "app.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../other.yaml", filepath.Join(variant, "app.yaml")); err != nil {
		t.Fatal(err)
	}
	out, _ = exec.Command("sh", "-c", packScript(dir, rev, "examples/app/releases/1.0")).CombinedOutput()
	if ok, detail := comparePack(string(out), rev); ok || !strings.Contains(detail, "modified examples/app/releases/1.0/app.yaml") {
		t.Fatalf("a repointed symlink: ok=%v %s", ok, detail)
	}
}
