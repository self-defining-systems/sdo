// Package gitio runs fixture Git commands for integration tests. It exposes no
// process handle, so a caller cannot start a command without waiting for it.
package gitio

import (
	"context"
	"os/exec"
)

// Run executes git in root and returns its combined output.
func Run(ctx context.Context, root string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...).CombinedOutput()
}
