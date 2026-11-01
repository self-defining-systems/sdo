// Package deploymentio owns deployment process, socket and timer adapters.
package deploymentio

import (
	"bytes"
	"context"
	"os/exec"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
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
func JobSuffix() string { return rand.String(8) }
func Evaluate(ctx context.Context, directory, binary string, args []string) ([]byte, []byte, error) {
	c := exec.CommandContext(ctx, binary, args...)
	c.Dir = directory
	var out, stderr bytes.Buffer
	c.Stdout, c.Stderr = &out, &stderr
	err := c.Run()
	return out.Bytes(), stderr.Bytes(), err
}
