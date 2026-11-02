// Package memoryio owns the operational-memory Git and syntax-check process edge.
// Callers pass revisions, arguments and input bytes; no process handles escape.
package memoryio

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

func Git(ctx context.Context, root string, combined bool, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}
	if combined {
		return cmd.CombinedOutput()
	}
	return cmd.Output()
}

func GitWithDiagnostics(ctx context.Context, root string, args ...string) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	return out, stderr.String(), err
}

func BashSyntax(ctx context.Context, script []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "bash", "-n")
	cmd.Stdin = bytes.NewReader(script)
	return cmd.CombinedOutput()
}

func GoAvailable() error    { _, err := exec.LookPath("go"); return err }
func Exited(err error) bool { var exit *exec.ExitError; return errors.As(err, &exit) }

// Getenv reads one process environment variable.
func Getenv(key string) string { return os.Getenv(key) }

// RandomText returns a fresh random, filename-safe token.
func RandomText() string { return rand.Text() }

// NewTicker starts a wall-clock ticker.
func NewTicker(interval time.Duration) *time.Ticker { return time.NewTicker(interval) }

// StreamGit runs git in root with input on stdin and hands its standard output
// to consume. A consumer failure kills the process; stderr is part of the error.
func StreamGit(ctx context.Context, root string, input []byte, consume func(io.Reader) error, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	consumeErr := consume(output)
	if consumeErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if consumeErr != nil {
		return consumeErr
	}
	if waitErr != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}
