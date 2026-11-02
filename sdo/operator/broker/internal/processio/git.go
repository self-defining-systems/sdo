package processio

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func RunGitEnv(ctx context.Context, cwd string, env []string, args ...string) ([]byte, error) {
	// Waiting for Git alone does not wait for its detached auto-maintenance.
	// Keep housekeeping inside this command's context and process lifetime.
	commandArgs := append([]string{"-c", "gc.autoDetach=false", "-c", "maintenance.autoDetach=false"}, args...)
	cmd := exec.CommandContext(ctx, "git", commandArgs...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}
