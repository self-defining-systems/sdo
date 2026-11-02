package lifecycleio

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"time"
)

// Command is the process launcher for accepted-repository Git reads and
// compiled working programs.
func Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}

// Getenv reads one environment variable at the runtime edge.
func Getenv(name string) string { return os.Getenv(name) }

// Since measures production elapsed time.
func Since(start time.Time) time.Duration { return time.Since(start) }

// Until measures production time remaining.
func Until(deadline time.Time) time.Duration { return time.Until(deadline) }

// After waits on the production timer clock.
func After(delay time.Duration) <-chan time.Time { return time.After(delay) }

// NewTimer starts a production timer.
func NewTimer(delay time.Duration) *time.Timer { return time.NewTimer(delay) }

// NewTicker starts a production ticker.
func NewTicker(interval time.Duration) *time.Ticker { return time.NewTicker(interval) }

// HTTPClient is the Kubernetes API transport client type.
type HTTPClient = http.Client
