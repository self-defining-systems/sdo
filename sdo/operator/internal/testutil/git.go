package testutil

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Git runs and waits for a fixture command. It never exposes an exec.Cmd that
// a caller could start without waiting before testing.T's TempDir cleanup.
func Git(t testing.TB, root string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v: %s", args, root, err, output)
	}
	return strings.TrimSpace(string(output))
}

// InitGit initializes an existing temporary directory before any fixture commit.
// A waited commit can otherwise spawn detached GC that races TempDir cleanup.
// Local configuration also covers production Git commands and linked worktrees,
// and gives the repository a commit identity so fixtures never depend on the
// host's global Git configuration.
func InitGit(t testing.TB, root string) {
	t.Helper()
	Git(t, root, "init", "-q")
	configureGit(t, root)
}

// CloneGit applies the same lifetime policy when a fixture needs an independent
// repository rather than a linked worktree (clone does not copy local config).
func CloneGit(t testing.TB, source, destination string, options ...string) {
	t.Helper()
	args := []string{"clone", "--quiet"}
	// Clone can run maintenance itself. Write the lifetime policy before its
	// first fetch, rather than configuring the repository after it returns.
	for _, setting := range gitFixtureConfig() {
		args = append(args, "--config", setting[0]+"="+setting[1])
	}
	args = append(args, options...)
	Git(t, source, append(args, source, destination)...)
}

func configureGit(t testing.TB, root string) {
	t.Helper()
	for _, setting := range gitFixtureConfig() {
		Git(t, root, "config", "--local", setting[0], setting[1])
	}
}

func gitFixtureConfig() [][2]string {
	return append(append([][2]string{}, gitLifetimeConfig...), gitIdentityConfig...)
}

var gitLifetimeConfig = [][2]string{
	{"gc.auto", "0"},
	{"maintenance.auto", "false"},
	{"gc.autoDetach", "false"},
	{"maintenance.autoDetach", "false"},
}

var gitIdentityConfig = [][2]string{
	{"user.name", "SDO Fixture"},
	{"user.email", "fixture@example.invalid"},
}
