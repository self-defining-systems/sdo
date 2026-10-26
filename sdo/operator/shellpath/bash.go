package shellpath

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type BashExecutor struct {
	Timeout time.Duration
	Now     func() time.Time
	Env     []string
}

func (e BashExecutor) Execute(ctx context.Context, root string, script Script) Result {
	now := e.Now
	if now == nil {
		now = time.Now
	}
	start := now()
	r := Result{Script: script.Path, ExitCode: -1, At: start.UTC()}
	if !filepath.IsLocal(script.Path) || strings.Contains(script.Path, "\\") {
		r.Error = "unsafe detector script path"
		return r
	}
	timeout := e.Timeout
	if timeout <= 0 {
		timeout = ScriptTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	capture := &tailCapture{}
	source := script.source
	if source == nil {
		var err error
		source, err = os.ReadFile(filepath.Join(root, filepath.FromSlash(script.Path)))
		if err != nil {
			r.Error = "detector source unavailable"
			return r
		}
	}
	// Discovery freezes the accepted main script in memory. Writable /tmp files
	// cannot change which authored bytes a later invocation executes.
	cmd := exec.CommandContext(runCtx, "bash", "-s")
	cmd.Args[0] = filepath.Join(root, filepath.FromSlash(script.Path))
	cmd.Stdin = bytes.NewReader(source)
	cmd.Dir = root
	cmd.Env = e.Env
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
	cmd.Stdout = tailWriter{capture, false}
	cmd.Stderr = tailWriter{capture, true}
	err := cmd.Run()
	// A successful shell can leave background processes behind. Retire the whole
	// invocation group on every exit, including success, so sweeps cannot leak work.
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	r.Duration = now().Sub(start)
	r.At = now().UTC()
	r.Stdout, r.Stderr, r.Truncated = capture.result()
	switch {
	case runCtx.Err() != nil:
		r.TimedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
		r.Error = runCtx.Err().Error()
	case err == nil:
		r.ExitCode = 0
	default:
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() >= 0 {
			r.ExitCode = exit.ExitCode()
		} else {
			r.Error = "detector process failed"
		}
	}
	return r
}

type outputChunk struct {
	stderr bool
	data   []byte
}
type tailCapture struct {
	mu        sync.Mutex
	chunks    []outputChunk
	size      int
	truncated bool
}
type tailWriter struct {
	capture *tailCapture
	stderr  bool
}

func (w tailWriter) Write(p []byte) (int, error) {
	c := w.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(p)
	if len(p) > MaxOutputBytes {
		p = p[len(p)-MaxOutputBytes:]
		c.truncated = true
	}
	c.chunks = append(c.chunks, outputChunk{w.stderr, append([]byte(nil), p...)})
	c.size += len(p)
	for c.size > MaxOutputBytes {
		drop := min(c.size-MaxOutputBytes, len(c.chunks[0].data))
		c.chunks[0].data = c.chunks[0].data[drop:]
		c.size -= drop
		c.truncated = true
		if len(c.chunks[0].data) == 0 {
			c.chunks = c.chunks[1:]
		}
	}
	return n, nil
}
func (c *tailCapture) result() (string, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var stdout, stderr strings.Builder
	for _, chunk := range c.chunks {
		if chunk.stderr {
			stderr.Write(chunk.data)
		} else {
			stdout.Write(chunk.data)
		}
	}
	return stdout.String(), stderr.String(), c.truncated
}
