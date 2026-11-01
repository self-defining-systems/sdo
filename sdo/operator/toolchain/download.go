package toolchain

import (
	"context"
	"os/exec"
)

// DownloadModules prepares the transitive module cache used by offline compiler
// checks. Process handles stay within the toolchain edge; callers receive the
// original combined diagnostics and cancellation error.
func DownloadModules(ctx context.Context, root string) ([]byte, error) {
	command := exec.CommandContext(ctx, "go", "mod", "download", "all")
	command.Dir = root
	return command.CombinedOutput()
}
