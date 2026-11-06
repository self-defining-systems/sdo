// Package gitio owns the Git process and scratch-directory edge of patchset.
package gitio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

// Call is one git invocation. Env replaces the environment entirely, except
// that PATH is inherited so the git binary and its helpers resolve.
type Call struct {
	Dir   string
	Env   []string
	Args  []string
	Stdin []byte
}

// Output is a finished git invocation. A non-zero exit is reported through
// ExitCode, not as an error.
type Output struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// Process is the production edge: real git processes and a temporary directory.
type Process struct{}

// Run starts git and waits for it. The error is non-nil only when the process
// could not run to an exit status (missing binary, cancelled context).
func (Process) Run(ctx context.Context, call Call) (Output, error) {
	cmd := exec.CommandContext(ctx, "git", call.Args...)
	cmd.Dir = call.Dir
	cmd.Env = append(append([]string(nil), call.Env...), "PATH="+os.Getenv("PATH"))
	if call.Stdin != nil {
		cmd.Stdin = bytes.NewReader(call.Stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	out := Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		out.ExitCode = exit.ExitCode()
		return out, nil
	}
	return out, err
}

// Scratch creates a temporary directory holding an empty "objects" directory (a
// private object database) and returns it with its remover.
func (Process) Scratch() (string, func(), error) {
	dir, err := os.MkdirTemp("", "sdo-patchset-")
	if err != nil {
		return "", nil, err
	}
	remove := func() { _ = os.RemoveAll(dir) }
	if err := os.Mkdir(filepath.Join(dir, "objects"), 0o755); err != nil {
		remove()
		return "", nil, err
	}
	return dir, remove, nil
}
