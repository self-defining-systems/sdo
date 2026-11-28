// Package renderio owns the rendered-object source Git process edge.
package renderio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Git reads from the repository at root and returns standard output.
func Git(ctx context.Context, root string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	return cmd.Output()
}

// MkTree writes a tree object from ls-tree style records ("mode type id\tname",
// one level) into the repository at root and returns its ID. Nothing reads
// configuration: no system or global files.
func MkTree(ctx context.Context, root string, records []string) (string, error) {
	var stdin bytes.Buffer
	for _, r := range records {
		stdin.WriteString(r)
		stdin.WriteByte(0)
	}
	cmd := exec.CommandContext(ctx, "git", "-C", root, "mktree", "-z")
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	cmd.Stdin = &stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return "", fmt.Errorf("git mktree: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
