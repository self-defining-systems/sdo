package lifecycleio

import (
	"context"
	"errors"
	"os/exec"
)

// GitOutput reads accepted repository bytes without exposing a process handle.
func GitOutput(ctx context.Context, repository string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).Output()
}

// GitCombined retains stderr diagnostics for repository identity checks.
func GitCombined(ctx context.Context, repository string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).CombinedOutput()
}

// GitEqual interprets diff's status 1 as a confirmed difference, preserving
// infrastructure failures as errors rather than treating them as source drift.
func GitEqual(ctx context.Context, repository string, args ...string) (bool, error) {
	err := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).Run()
	if err == nil {
		return true, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}
