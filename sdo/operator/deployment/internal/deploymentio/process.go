// Package deploymentio owns deployment process, socket and timer adapters.
package deploymentio

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"
)

func Run(ctx context.Context, name string, args []string, input []byte) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...)
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
	c := exec.CommandContext(ctx, binary, args...)
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
	return exec.CommandContext(ctx, name, args...).Output()
}

// Now is the rollout clock.
func Now() time.Time { return time.Now() }
