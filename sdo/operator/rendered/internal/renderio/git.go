// Package renderio owns the rendered-object source Git process edge.
package renderio

import (
	"context"
	"os/exec"
)

// Git reads from the repository at root and returns standard output.
func Git(ctx context.Context, root string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...).Output()
}
