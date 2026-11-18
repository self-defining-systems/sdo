//go:build integration

package processio

import (
	"context"
	"syscall"
	"testing"
	"time"
)

// The first SIGTERM asks for a graceful stop; a second one asks to stop now.
func TestSecondSignalEscalates(t *testing.T) {
	ctx, force, stop := Termination(context.Background())
	defer stop()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the first SIGTERM did not end the context")
	}
	select {
	case <-force:
		t.Fatal("one signal must not force the stop")
	case <-time.After(200 * time.Millisecond):
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-force:
	case <-time.After(5 * time.Second):
		t.Fatal("the second signal did not escalate")
	}
}
