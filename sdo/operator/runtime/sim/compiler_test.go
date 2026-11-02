package sim_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/runtime/sim"
)

func TestCompilerRecordsCallsAndRejectsUnparsableGo(t *testing.T) {
	stage := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(stage, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stage, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ok.go", "package p\n")
	write("bad.go", "package p\nfunc (\n")
	write("README.md", "not go (\n")
	var c sim.Compiler
	ctx := context.Background()
	if err := c.Compile(ctx, stage, []string{"ok.go", "README.md", "gone.go"}); err != nil {
		t.Fatalf("parsable, non-Go and deleted paths must pass: %v", err)
	}
	if err := c.Compile(ctx, stage, []string{"bad.go"}); err == nil || !strings.Contains(err.Error(), "bad.go") {
		t.Fatalf("unparsable Go accepted: %v", err)
	}
	sentinel := errors.New("rule")
	c.Verdict = func(call sim.CompileCall) error { return sentinel }
	if err := c.Compile(ctx, stage, nil); !errors.Is(err, sentinel) {
		t.Fatalf("injected verdict lost: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.Compile(cancelled, stage, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	if got := c.Calls(); len(got) != 4 || got[0].Changed[0] != "ok.go" {
		t.Fatalf("calls = %+v", got)
	}
}
