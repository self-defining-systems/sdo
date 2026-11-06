package builder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/builder/internal/buildio"
	"sdo.dev/operator/memory"
)

func exitError(t *testing.T, code int) error {
	t.Helper()
	script := filepath.Join(t.TempDir(), "child.sh")
	if err := os.WriteFile(script, []byte(fmt.Sprintf("#!/bin/sh\nexit %d\n", code)), 0o755); err != nil {
		t.Fatal(err)
	}
	return buildio.RunController(context.Background(), script, "unused.json", t.TempDir())
}

func TestNeedsRebuildIsExactlyTheRefreshExitCode(t *testing.T) {
	if !NeedsRebuild(exitError(t, memory.HealthRefreshExitCode)) {
		t.Fatal("exit 76 must request a rebuild")
	}
	for _, code := range []int{1, 2, 75, 77} {
		if NeedsRebuild(exitError(t, code)) {
			t.Fatalf("exit %d must not request a rebuild", code)
		}
	}
	if NeedsRebuild(nil) || NeedsRebuild(errors.New("x")) {
		t.Fatal("non-exit errors must not request a rebuild")
	}
}

// Each adoption used to end the pod; the Job gives up after six pod failures.
func TestConsecutiveAdoptionsStayInOneProcess(t *testing.T) {
	refresh := exitError(t, memory.HealthRefreshExitCode)
	const adoptions = 10 // more than the Job's backoff limit of 6
	passes := 0
	err := runLoop(context.Background(), func(context.Context) (string, error) {
		passes++
		if passes <= adoptions {
			return fmt.Sprintf("rev-%d", passes), fmt.Errorf("controller run: %w", refresh)
		}
		return "rev-final", nil
	}, RestartPolicy{Sleep: func(context.Context, time.Duration) error { t.Fatal("progress must not wait"); return nil }})
	if err != nil || passes != adoptions+1 {
		t.Fatalf("passes=%d err=%v; want %d passes and no error", passes, err, adoptions+1)
	}
}

func TestOtherFailuresEndTheLoop(t *testing.T) {
	boom := errors.New("boom")
	passes := 0
	err := runLoop(context.Background(), func(context.Context) (string, error) { passes++; return "r", boom }, RestartPolicy{})
	if !errors.Is(err, boom) || passes != 1 {
		t.Fatalf("passes=%d err=%v", passes, err)
	}
}

func TestRepeatedRequestAtSameRevisionBacksOff(t *testing.T) {
	refresh := exitError(t, memory.HealthRefreshExitCode)
	passes := 0
	var waits []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	err := runLoop(ctx, func(context.Context) (string, error) {
		passes++
		if passes == 6 {
			cancel()
		}
		return "stuck", refresh
	}, RestartPolicy{Backoff: time.Second, MaxBackoff: 4 * time.Second, Sleep: func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits=%v want %v", waits, want)
	}
}
