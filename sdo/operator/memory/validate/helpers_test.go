package validate

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// copyTree copies a tree and preserves permission bits, which the script
// executable rule depends on.
func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// goodTree is a copy of the valid fixture memory tree.
func goodTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join("testdata", "good"), root)
	return root
}

// applyCase overlays testdata/bad/<name> onto root. A DELETE file lists
// repository-relative paths to remove first.
func applyCase(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join("testdata", "bad", name)
	if data, err := os.ReadFile(filepath.Join(dir, "DELETE")); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				if err := os.RemoveAll(filepath.Join(root, line)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	staging := t.TempDir()
	copyTree(t, dir, staging)
	if err := os.Remove(filepath.Join(staging, "DELETE")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	copyTree(t, staging, root)
}

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func remove(t *testing.T, root, rel string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
		t.Fatal(err)
	}
}

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

func diagnose(t *testing.T, request Request) []Diagnostic {
	t.Helper()
	diagnostics, err := Validate(context.Background(), request)
	if err != nil {
		t.Fatalf("Validate returned an infrastructure error: %v", err)
	}
	return diagnostics
}

func ruleSet(diagnostics []Diagnostic) []string {
	seen := map[string]bool{}
	for _, d := range diagnostics {
		seen[d.Rule] = true
	}
	rules := make([]string, 0, len(seen))
	for rule := range seen {
		rules = append(rules, rule)
	}
	sort.Strings(rules)
	return rules
}

func same(got, want []string) bool {
	want = append([]string{}, want...)
	sort.Strings(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}
