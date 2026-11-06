// Package renderio owns the rendered-object source Git process edge.
package renderio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Git reads from the repository at root and returns standard output.
func Git(ctx context.Context, root string, args ...string) ([]byte, error) {
	return GitEnv(ctx, root, nil, args...)
}

// GitEnv is Git with extra environment entries (a private object directory
// and its alternates, for an effective tree that is not in the accepted store).
func GitEnv(ctx context.Context, root string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	return cmd.Output()
}

// Project writes the tree of `tree` without the paths in drop, through a
// scratch index, and returns its ID. env names the object database (it must
// already hold tree); the new trees are written to its object directory.
// Nothing reads configuration: no system or global files, a scratch HOME.
func Project(ctx context.Context, root string, env []string, tree string, drop []string) (string, error) {
	scratch, err := os.MkdirTemp("", "sdo-project-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(scratch) }()
	env = append(append([]string(nil), env...),
		"GIT_INDEX_FILE="+filepath.Join(scratch, "index"),
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "HOME="+scratch)
	run := func(stdin []byte, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root, "--literal-pathspecs"}, args...)...)
		cmd.Env = append(os.Environ(), env...)
		if stdin != nil {
			cmd.Stdin = bytes.NewReader(stdin)
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
		}
		return out, err
	}
	if _, err := run(nil, "read-tree", tree); err != nil {
		return "", err
	}
	if len(drop) > 0 {
		var list bytes.Buffer
		for _, p := range drop {
			list.WriteString(p)
			list.WriteByte(0)
		}
		if _, err := run(list.Bytes(), "rm", "-r", "-q", "--cached", "--ignore-unmatch", "--pathspec-from-file=-", "--pathspec-file-nul"); err != nil {
			return "", err
		}
	}
	out, err := run(nil, "write-tree")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
