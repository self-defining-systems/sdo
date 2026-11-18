package buildio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// StopGrace is how long a stopping controller child may take to shut down
// (its 5 s leader Lease release included) before it is killed. The controller
// pod's terminationGracePeriodSeconds must exceed it.
const StopGrace = 20 * time.Second

// RunController runs the controller child until it exits. When ctx ends the
// child receives SIGTERM, so it can release its leader Lease, and is killed
// only if it is still running StopGrace later.
func RunController(ctx context.Context, binary, configPath, directory string) error {
	return runController(ctx, binary, configPath, directory, StopGrace)
}

func runController(ctx context.Context, binary, configPath, directory string, grace time.Duration) error {
	command := exec.CommandContext(ctx, binary, "--config", configPath)
	command.Cancel = func() error { return command.Process.Signal(syscall.SIGTERM) }
	command.WaitDelay = grace
	command.Dir = directory
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("controller run: %w", err)
	}
	finished := make(chan struct{})
	defer close(finished)
	if force, _ := ctx.Value(forcedStopKey{}).(<-chan struct{}); force != nil {
		go func() {
			select {
			case <-force:
				_ = command.Process.Kill()
			case <-finished:
			}
		}()
	}
	if err := command.Wait(); err != nil {
		return fmt.Errorf("controller run: %w", err)
	}
	return nil
}

type forcedStopKey struct{}

// WithForcedStop returns a context that carries a channel whose closing makes
// RunController kill the child at once, instead of waiting out StopGrace (a
// second SIGTERM or SIGINT on the parent).
func WithForcedStop(ctx context.Context, force <-chan struct{}) context.Context {
	return context.WithValue(ctx, forcedStopKey{}, force)
}

// Stopped reports whether err is the child ending because it was asked to
// stop: its terminated status (128+SIGTERM) or its death by a signal.
func Stopped(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && (exit.ExitCode() == 143 || exit.ExitCode() == -1)
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
