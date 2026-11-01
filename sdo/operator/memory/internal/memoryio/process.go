// Package memoryio owns the operational-memory Git and syntax-check process edge.
// Callers pass revisions, arguments and input bytes; no process handles escape.
package memoryio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
)

func Git(ctx context.Context, root string, combined bool, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}
	if combined {
		return cmd.CombinedOutput()
	}
	return cmd.Output()
}

func GitWithDiagnostics(ctx context.Context, root string, args ...string) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	return out, stderr.String(), err
}

func BashSyntax(ctx context.Context, script []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "bash", "-n")
	cmd.Stdin = bytes.NewReader(script)
	return cmd.CombinedOutput()
}

func GoAvailable() error    { _, err := exec.LookPath("go"); return err }
func Exited(err error) bool { var exit *exec.ExitError; return errors.As(err, &exit) }
