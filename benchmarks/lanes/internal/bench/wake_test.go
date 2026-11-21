//go:build integration

package bench

import (
	"context"
	"os"
	"testing"
	"time"
)

// `plan add` wakes a running manager: the next pass starts within a poll, not after the manager's tick.
func TestAWakeRequestEndsTheWaitForTheNextPass(t *testing.T) {
	root := Root(t.TempDir())
	m := &Manager{Root: root, Tick: time.Hour}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = Wake(root)
	}()
	start := time.Now()
	if !m.waitTick(context.Background()) {
		t.Fatal("waitTick said the context ended")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the manager waited %s after a wake", took)
	}
	if _, err := os.Stat(root.wakePath()); err == nil {
		t.Fatal("the wake request was not consumed")
	}
}

func TestWaitTickEndsWithItsContext(t *testing.T) {
	m := &Manager{Root: Root(t.TempDir()), Tick: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if m.waitTick(ctx) {
		t.Fatal("waitTick carried on after its context ended")
	}
}
