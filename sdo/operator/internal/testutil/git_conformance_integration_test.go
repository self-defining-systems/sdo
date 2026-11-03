//go:build integration

package testutil

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil/gitio"
)

// There is no fake git: tests run the real binary through Git, InitGit and
// CloneGit, which only add a waited command and repository-local maintenance
// and identity settings. This table shows the helpers change nothing a test
// observes: one command sequence runs in a helper repository and in a plain
// `git init` repository, and every query returns the same text.
func TestGitHelpersObserveWhatPlainGitObserves(t *testing.T) {
	steps := [][]string{
		{"checkout", "-q", "-b", "main"},
		{"add", "-A"},
		{"commit", "-qm", "first"},
		{"branch", "feature"},
		{"checkout", "-q", "feature"},
		{"mv", "a.txt", "b.txt"},
		{"commit", "-qam", "rename"},
	}
	queries := [][]string{
		{"log", "--format=%s", "--all"},
		{"branch", "--show-current"},
		{"status", "--porcelain"},
		{"ls-files"},
		{"diff", "--name-status", "-M", "main", "feature"},
		{"show", "main:a.txt"},
		{"rev-parse", "--is-inside-work-tree"},
		{"for-each-ref", "--format=%(refname)"},
	}
	build := func(root string, run func(args ...string) string) {
		if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("one\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, step := range steps {
			run(step...)
		}
	}

	helper := t.TempDir()
	InitGit(t, helper)
	build(helper, func(args ...string) string { return Git(t, helper, args...) })

	plain := t.TempDir()
	plainRun := func(args ...string) string {
		command := exec.Command("git", append([]string{"-C", plain, "-c", "user.name=Plain", "-c", "user.email=plain@example.invalid"}, args...)...)
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	plainRun("init", "-q")
	build(plain, plainRun)

	for _, query := range queries {
		if got, want := Git(t, helper, query...), plainRun(query...); got != want {
			t.Errorf("git %v: helper repository says %q, plain git says %q", query, got, want)
		}
	}

	// A failing command fails the same way in both repositories.
	missing := []string{"rev-parse", "--verify", "no-such-ref^{commit}"}
	_, helperErr := gitio.Run(context.Background(), helper, missing...)
	_, plainErr := exec.Command("git", append([]string{"-C", plain}, missing...)...).CombinedOutput()
	if helperErr == nil || plainErr == nil {
		t.Fatalf("a missing ref must fail in both: helper %v, plain %v", helperErr, plainErr)
	}
}
