// Package workerio owns the worker registry's process, socket, timer, clock,
// randomness and environment adapters.
package workerio

import (
	"context"
	"crypto/rand"
	"net"
	"os"
	"os/exec"
	"time"
)

// Getenv reads one process environment variable.
func Getenv(key string) string { return os.Getenv(key) }

// Environ returns the process environment.
func Environ() []string { return os.Environ() }

// Now reads the wall clock.
func Now() time.Time { return time.Now() }

// Since reports the wall-clock time elapsed since t.
func Since(t time.Time) time.Duration { return time.Since(t) }

// Until reports the wall-clock time remaining until t.
func Until(t time.Time) time.Duration { return time.Until(t) }

// NewTicker starts a wall-clock ticker.
func NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// NewTimer starts a wall-clock timer.
func NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// After waits for d on the wall clock.
func After(d time.Duration) <-chan time.Time { return time.After(d) }

// RandomRead fills b with cryptographically secure random bytes.
func RandomRead(b []byte) error { _, err := rand.Read(b); return err }

// Dial reports whether address accepts a TCP connection within timeout.
func Dial(address string, timeout time.Duration) error {
	connection, err := net.DialTimeout("tcp", address, timeout)
	if err == nil {
		connection.Close()
	}
	return err
}

// CombinedOutput runs a short-lived process and returns its combined output.
func CombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// Worker is a started registered-agent process.
type Worker struct{ cmd *exec.Cmd }

// StartWorker launches command in dir with env and two inherited descriptors,
// forwarding its output to this process's output.
func StartWorker(ctx context.Context, command []string, dir string, env []string, ready, claim *os.File) (*Worker, error) {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	cmd.ExtraFiles = []*os.File{ready, claim}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Worker{cmd: cmd}, nil
}

// Wait waits for the worker to exit.
func (w *Worker) Wait() error { return w.cmd.Wait() }

// Kill terminates the worker and reaps it.
func (w *Worker) Kill() {
	_ = w.cmd.Process.Kill()
	_ = w.cmd.Wait()
}
