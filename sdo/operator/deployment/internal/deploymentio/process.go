// Package deploymentio owns deployment process, socket and timer adapters.
package deploymentio

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"
)

// pipeDrainDelay bounds how long a finished or cancelled process may keep its
// output pipes open through a descendant (a memory host in its own process
// group survives its parent's kill) before the call returns anyway.
const pipeDrainDelay = 2 * time.Second

func command(ctx context.Context, name string, args []string) *exec.Cmd {
	c := exec.CommandContext(ctx, name, args...)
	c.WaitDelay = pipeDrainDelay
	return c
}

func Run(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
	c := command(ctx, name, args)
	if input != nil {
		c.Stdin = bytes.NewReader(input)
	}
	return c.CombinedOutput()
}
func Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Evaluate runs binary in directory with the process environment plus extraEnv.
func Evaluate(ctx context.Context, directory, binary string, args, extraEnv []string) ([]byte, []byte, error) {
	c := command(ctx, binary, args)
	c.Dir = directory
	if len(extraEnv) > 0 {
		c.Env = append(os.Environ(), extraEnv...)
	}
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	err := c.Run()
	return out.Bytes(), stderr.Bytes(), err
}

// Output runs a process and returns its standard output.
func Output(ctx context.Context, name string, args []string) ([]byte, error) {
	return command(ctx, name, args).Output()
}

// Now is the rollout clock.
func Now() time.Time { return time.Now() }
