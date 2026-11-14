// Package kindio is the sandbox provider's process and timer edge: it runs the
// kind and docker binaries and waits between polls.
package kindio

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// Run executes name with args and returns its combined output. Failures carry
// the output, so the caller's error says why kind or docker refused.
func Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.Env = os.Environ()
	err := cmd.Run()
	if err != nil {
		return out.Bytes(), fmt.Errorf("%s %v: %w: %s", name, args, err, tail(out.Bytes()))
	}
	return out.Bytes(), nil
}

func tail(b []byte) string {
	const limit = 2000
	if len(b) > limit {
		b = b[len(b)-limit:]
	}
	return string(b)
}

// Sleep waits d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Now is the wall clock.
func Now() time.Time { return time.Now() }
