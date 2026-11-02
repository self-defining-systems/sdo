// Package shellio owns the shell detector path's process, socket, timer and
// environment adapters. Callers pass scripts, revisions and handlers; no
// process handles escape.
package shellio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// Now reads the wall clock.
func Now() time.Time { return time.Now() }

// Environ returns the process environment.
func Environ() []string { return os.Environ() }

// Getenv reads one process environment variable.
func Getenv(key string) string { return os.Getenv(key) }

// NewTicker starts a wall-clock ticker.
func NewTicker(interval time.Duration) *time.Ticker { return time.NewTicker(interval) }

// Wait blocks for d or until ctx ends.
func Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NewHTTPClient returns a client with an overall request timeout that never
// follows redirects.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// Server serves an HTTP handler on a TCP address until closed.
type Server struct {
	server *http.Server
	errors chan error
}

// Listen starts serving handler on address.
func Listen(address string, handler http.Handler) *Server {
	s := &Server{server: &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}, errors: make(chan error, 1)}
	go func() { s.errors <- s.server.ListenAndServe() }()
	return s
}

// Errors reports why the server stopped.
func (s *Server) Errors() <-chan error { return s.errors }

// Close stops the server immediately.
func (s *Server) Close() error { return s.server.Close() }

// RunBash feeds source to a bash reading its program from stdin, named argv0,
// in root as its own process group. Every exit retires the group. A nil error
// means exit status 0; an *ExitError carries the status.
func RunBash(ctx context.Context, root, argv0 string, source []byte, env []string, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, "bash", "-s")
	cmd.Args[0] = argv0
	cmd.Stdin = bytes.NewReader(source)
	cmd.Dir = root
	cmd.Env = env
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	// A successful shell can leave background processes behind.
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	return err
}

// ExitCode extracts a non-negative process exit status from err.
func ExitCode(err error) (int, bool) {
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() >= 0 {
		return exit.ExitCode(), true
	}
	return 0, false
}

// Join builds the in-repository path of a script for argv0.
func Join(root, rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }

// Head returns the revision checked out at root.
func Head(ctx context.Context, root string) ([]byte, error) {
	return exec.CommandContext(ctx, "git", "-c", "safe.directory=*", "-C", root, "rev-parse", "HEAD").Output()
}

// Archive streams the tar archive of commit to consume. A consumer failure
// kills the process.
func Archive(ctx context.Context, root, commit string, consume func(io.Reader) error) error {
	archive := exec.CommandContext(ctx, "git", "-c", "safe.directory=*", "-C", root, "archive", "--format=tar", commit)
	pipe, err := archive.StdoutPipe()
	if err != nil {
		return err
	}
	if err = archive.Start(); err != nil {
		return err
	}
	consumed := consume(pipe)
	if consumed != nil {
		_ = archive.Process.Kill()
	}
	waitErr := archive.Wait()
	if consumed != nil {
		return consumed
	}
	return waitErr
}
