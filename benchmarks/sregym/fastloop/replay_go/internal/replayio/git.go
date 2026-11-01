// Package replayio owns benchmark replay Git processes. No process handles escape.
package replayio

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func Git(ctx context.Context, root string, args ...string) error {
	argv := append([]string{}, args...)
	if root != "" {
		argv = append([]string{"-C", root}, args...)
	}
	command := exec.CommandContext(ctx, "git", argv...)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}
