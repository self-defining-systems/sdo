//go:build integration

// Package rehearsalio is the replay rehearsal's host edge: the kubectl
// process the deployer's command backend runs against the envtest API
// server, the real-time poll of the fake garbage collector, and the host
// wall clock that measures the harness's own cost. Integration tests only.
package rehearsalio

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"
)

// Run runs name with args and input, with KUBECONFIG set to kubeconfig, and
// returns its combined output (the production command backend's contract).
func Run(ctx context.Context, kubeconfig, name string, args []string, input []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	return cmd.CombinedOutput()
}

// Every calls f now and then every interval of real time until ctx ends.
func Every(ctx context.Context, interval time.Duration, f func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		f()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Stopwatch returns a function that reports the host time elapsed since it
// was started.
func Stopwatch() func() time.Duration {
	started := time.Now()
	return func() time.Duration { return time.Since(started) }
}
