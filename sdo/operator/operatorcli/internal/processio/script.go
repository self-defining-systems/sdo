package processio

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"sdo.dev/operator/sdk/program"
)

// Environ returns the process environment.
func Environ() []string { return os.Environ() }

// Until reports how long remains before t on the wall clock.
func Until(t time.Time) time.Duration { return time.Until(t) }

// LocalServer is a loopback HTTP server on an ephemeral port.
type LocalServer struct {
	server   *http.Server
	listener net.Listener
}

// StartLocalServer serves handler on 127.0.0.1 until closed.
func StartLocalServer(handler http.Handler) (*LocalServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	return &LocalServer{server: server, listener: listener}, nil
}

// Addr is the host:port the server listens on.
func (s *LocalServer) Addr() string { return s.listener.Addr().String() }

// Shutdown stops accepting and waits for requests until ctx ends.
func (s *LocalServer) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }

// Close stops the server immediately.
func (s *LocalServer) Close() error { return s.server.Close() }

type scriptOutput struct {
	bytes     []byte
	truncated bool
	mu        sync.Mutex
}

func (w *scriptOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	space := program.MaxScriptOutputBytes - len(w.bytes)
	if len(p) > space {
		p = p[:space]
		w.truncated = true
	}
	w.bytes = append(w.bytes, p...)
	return n, nil
}

// ExecuteScript runs a bash script in its own process group and records its
// bounded output into out. It returns the script exit code.
func ExecuteScript(ctx context.Context, script string, env []string, out *program.ScriptExecutionResult) int32 {
	stdout, stderr := &scriptOutput{}, &scriptOutput{}
	command := exec.CommandContext(ctx, "bash", "-c", script)
	command.Env = env
	command.Stdout = stdout
	command.Stderr = stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	command.WaitDelay = 250 * time.Millisecond
	err := command.Run()
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	out.Stdout = string(stdout.bytes)
	out.Stderr = string(stderr.bytes)
	out.StdoutTruncated = stdout.truncated
	out.StderrTruncated = stderr.truncated
	out.TimedOut = ctx.Err() != nil
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		if code < 0 {
			code = 137
		}
		return int32(code)
	}
	return 127
}
