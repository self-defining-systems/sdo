//go:build integration

package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// LB13: a build that failed in `app up` said only "app up: exit 1". Every failed command now records its exit code and
// the end of its output (at most 40 lines) in the step result, the timeline and the lane reason.
func TestFailedBuildCarriesExitCodeAndOutputTail(t *testing.T) {
	l := newLab(t)
	l.set("app-up-fails", "1")
	l.addReusing("e1", "")
	l.until("the lane is down", func() bool { return l.lane().State == LaneDown })

	reason := l.lane().Reason
	for _, want := range []string{"app up", "exit 1", "app up line 50", "app up line 11"} {
		if !strings.Contains(reason, want) {
			t.Errorf("lane reason lacks %q: %q", want, reason)
		}
	}
	if strings.Contains(reason, "app up line 10 ") || strings.Contains(reason, "app up line 10|") {
		t.Errorf("lane reason carries more than the last 40 lines: %q", reason)
	}

	tl, _ := os.ReadFile(l.root.TimelinePath("c1"))
	if !strings.Contains(string(tl), "app up line 50") {
		t.Errorf("the lane timeline lacks the output tail of the failed app step: %s", tl)
	}
}

// The step result keeps the exit code, the capped tail and the log path next to the reason.
func TestFailCallFillsTheStepResult(t *testing.T) {
	var lines []string
	for i := 1; i <= 100; i++ {
		lines = append(lines, fmt.Sprintf("out %d", i))
	}
	var tail tailWriter
	tail.Write([]byte(strings.Join(lines, "\n")))
	dir := t.TempDir()
	r := &stepRunner{dir: dir, log: io.Discard}
	r.failCall("app up", 1, tail.String(), nil)
	if r.res.OK || r.res.Exit != 1 || r.res.Log != filepath.Join(dir, "runner.log") {
		t.Fatalf("result %+v", r.res)
	}
	if n := strings.Count(r.res.Tail, "\n") + 1; n != 40 || !strings.HasSuffix(r.res.Tail, "out 100") {
		t.Fatalf("tail has %d lines: %q", n, r.res.Tail)
	}
	if !strings.HasPrefix(r.res.Reason, "app up: exit 1: out 61 | ") || !strings.HasSuffix(r.res.Reason, "out 100") {
		t.Fatalf("reason %q", r.res.Reason)
	}
}

func TestTailTextCapsLinesAndBytes(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 100; i++ {
		fmt.Fprintf(&b, "line %d\n\n", i)
	}
	got := tailText(b.String())
	lines := strings.Split(got, "\n")
	if len(lines) != tailMaxLines || lines[0] != "line 61" || lines[len(lines)-1] != "line 100" {
		t.Fatalf("%d lines, first %q last %q", len(lines), lines[0], lines[len(lines)-1])
	}
	long := strings.Repeat("x", 100) + "\n"
	if got := tailText(strings.Repeat(long, 39)); len(got) > tailMaxBytes {
		t.Fatalf("%d bytes, cap %d", len(got), tailMaxBytes)
	}
}

func TestFailTextHasExitErrorAndTail(t *testing.T) {
	got := failText(2, "a\nb", errors.New("boom"))
	if got != "exit 2 boom: a | b" {
		t.Fatal(got)
	}
	if got := failText(1, "", nil); got != "exit 1" {
		t.Fatal(got)
	}
	r := HookResult{Exit: 1, Output: "p1\np2\nerror: x\n"}
	if got := r.Why("prepare"); got != "prepare exit 1: p1 | p2 | error: x" {
		t.Fatal(got)
	}
}

// A failed host command (cluster-up) reports its exit code and output, not only "exit status 3".
func TestFailedShellCommandReportsExitAndOutput(t *testing.T) {
	r := &stepRunner{dir: t.TempDir(), log: io.Discard, s: Step{Platform: t.TempDir()}}
	if r.shell(context.Background(), "cluster-up", "echo kubectl not found >&2; exit 3", nil) {
		t.Fatal("the command failed, shell must say so")
	}
	if r.res.Exit != 3 || !strings.Contains(r.res.Reason, "exit 3") || !strings.Contains(r.res.Reason, "kubectl not found") {
		t.Fatalf("exit %d reason %q", r.res.Exit, r.res.Reason)
	}
	tl, _ := os.ReadFile(filepath.Join(r.dir, "timeline.jsonl"))
	if !strings.Contains(string(tl), "kubectl not found") {
		t.Fatalf("timeline %s", tl)
	}
}
