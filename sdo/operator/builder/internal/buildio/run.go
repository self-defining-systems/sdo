package buildio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"
)

func RunController(ctx context.Context, binary, configPath, directory string) error {
	command := exec.CommandContext(ctx, binary, "--config", configPath)
	command.Dir = directory
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("controller run: %w", err)
	}
	return nil
}

// ExitCode returns the exit status of the process err reports, or -1 when err
// is not a process exit.
func ExitCode(err error) int {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}

// Sleep waits for d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
