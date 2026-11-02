package processio

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"time"
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

// Now reads the wall clock for publication race deadlines.
func Now() time.Time { return time.Now() }

// Backoff waits a jittered delay before the next publication attempt.
func Backoff(ctx context.Context, backoff time.Duration) error {
	timer := time.NewTimer(backoff/2 + rand.N(backoff))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
