package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/builder"
)

// watchScript prints one verdict line per second-stamp it is given, then stays
// alive (or exits non-zero when exit is set).
func watchScript(t *testing.T, body string) *PreparedProbe {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "evaluate")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return &PreparedProbe{workspace: &builder.Workspace{Directory: dir}, binary: path}
}

func TestWatcherReturnsOnlyVerdictsJudgedAfterTheRequestedTime(t *testing.T) {
	p := watchScript(t, `echo '{"findings":[],"status":"clear","started_at":"2026-01-01T00:00:01Z"}'
echo '{"findings":[],"status":"firing","started_at":"2026-01-01T00:00:09Z"}'
sleep 30`)
	w := p.Watch(context.Background(), time.Second)
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	early, err := w.Next(ctx, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || early.StartedAt.IsZero() {
		t.Fatalf("early: %+v, %v", early, err)
	}
	late, err := w.Next(ctx, time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC))
	if err != nil || late.Status != "firing" {
		t.Fatalf("a verdict judged before the request must not satisfy it: %+v, %v", late, err)
	}
}

func TestWatcherReportsAnEndedEvaluation(t *testing.T) {
	p := watchScript(t, `echo "boom: detector build failed" >&2
exit 3`)
	w := p.Watch(context.Background(), time.Second)
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := w.Next(ctx, time.Time{}.Add(time.Hour))
	if err == nil {
		t.Fatal("expected an error")
	}
	if want := "boom: detector build failed"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not carry the process's stderr", err)
	}
}
