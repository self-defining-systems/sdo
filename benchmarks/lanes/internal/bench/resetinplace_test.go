//go:build integration

package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// addOneSeq adds a hooked experiment of one sequence (a@x) with the given operator block and spec lines appended.
func (l *lab) addOneSeq(name, operator, extra string) {
	l.t.Helper()
	p := filepath.Join(l.specs, name+".yaml")
	must(l.t, os.WriteFile(p, []byte(fmt.Sprintf(labSpec, name, operator, "a@x", "[[1]]", l.ready, l.cap, 0)+extra), 0o644))
	must(l.t, Add(context.Background(), l.root, p, io.Discard))
}

// Rows of one operator profile run one after another on a lane without reinstalling the operator: the first row's
// build takes the golden copy, its release resets the lane to it, and the next row (another experiment, same profile)
// takes the lane by swap, with no teardown, bare-app gate or prepare. The swap runs the full build check (the reset
// keeps no ledger), so the second row verifies writes confirmed on its own lane state.
func TestBenchResetInPlaceCarriesTheOperatorToTheNextRow(t *testing.T) {
	l := newLab(t)
	l.addOneSeq("r1", l.hooked, "reuse_app: true\nkeep_lanes: true\nreset_in_place: true\n")
	l.until("r1 done", func() bool { return l.exp("r1").Count(0).Done == 1 })
	l.until("r1's lane reset to its golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneFree && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden && lf.Prepared.Reset
	})
	l.addOneSeq("r2", l.hooked, "reuse_app: true\nkeep_lanes: true\nreset_in_place: true\n")
	l.until("r2 done", func() bool { return l.exp("r2").Count(0).Done == 1 })
	hooks, log := l.read("hooks.log"), l.read("fuzzops.log")
	if n := strings.Count(hooks, "snapshot c1 r1"); n != 1 {
		t.Fatalf("%d snapshots by r1, want 1:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "swap c1 r2"); n != 1 {
		t.Fatalf("r2 swapped %d times, want 1:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "prepare c1 r2"); n != 0 {
		t.Fatalf("r2 prepared %d times, want 0 (the lane's operator is kept):\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "teardown c1 r1"); n != 1 {
		t.Fatalf("%d teardowns of r1's operator, want only its own build's (a reset lane is not torn down):\n%s", n, hooks)
	}
	if n := countLines(log, "check-without-operator"); n != 1 {
		t.Fatalf("%d bare-app gates, want only r1's build's:\n%s", n, log)
	}
	if n := countLines(log, "check-with-operator"); n != 2 {
		t.Fatalf("%d build checks with the operator, want 2 (r1's build and r2's swap: no ledger is carried):\n%s", n, log)
	}
	if n := countLines(log, "app up"); n != 1 {
		t.Fatalf("%d app copies, want 1:\n%s", n, log)
	}
	if _, err := os.Stat(filepath.Join(l.staging, "c1", "lane", ledgerGolden)); err == nil {
		t.Fatal("a lane reset in place kept a golden ledger")
	}
}

// A row of another operator profile on a lane reset in place tears the kept operator down and builds as before.
func TestBenchResetInPlaceLaneGoesToAnotherProfileByTeardown(t *testing.T) {
	l := newLab(t)
	l.addOneSeq("r1", l.hooked, "reuse_app: true\nkeep_lanes: true\nreset_in_place: true\n")
	l.until("r1's lane reset to its golden copy", func() bool {
		lf := l.lane()
		return l.exp("r1").Count(0).Done == 1 && lf.State == LaneFree && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Reset
	})
	other := strings.Replace(l.hooked, "startup_bound: 0s", "startup_bound: 0s, model: other", 1)
	l.addOneSeq("v1", other, "reuse_app: true\nkeep_lanes: true\n")
	l.until("v1 done", func() bool { return l.exp("v1").Count(0).Done == 1 })
	hooks := l.read("hooks.log")
	if n := strings.Count(hooks, "swap c1 v1"); n != 0 {
		t.Fatalf("a row of another profile took the lane by swap:\n%s", hooks)
	}
	if n := strings.Count(hooks, "prepare c1 v1"); n != 1 {
		t.Fatalf("v1 prepared %d times, want 1:\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "teardown c1 r1"); n < 2 {
		t.Fatalf("r1's kept operator was not torn down before v1's build:\n%s", hooks)
	}
}

// Without reset_in_place every row reinstalls the operator: the default is unchanged.
func TestBenchWithoutResetInPlaceEachRowPrepares(t *testing.T) {
	l := newLab(t)
	l.addOneSeq("r1", l.hooked, "reuse_app: true\nkeep_lanes: true\n")
	l.until("r1 done", func() bool { return l.exp("r1").Count(0).Done == 1 })
	l.addOneSeq("r2", l.hooked, "reuse_app: true\nkeep_lanes: true\n")
	l.until("r2 done", func() bool { return l.exp("r2").Count(0).Done == 1 })
	hooks := l.read("hooks.log")
	if strings.Contains(hooks, "snapshot c1 ") || strings.Contains(hooks, "swap c1 ") {
		t.Fatalf("a row without reset_in_place took a golden copy or swapped:\n%s", hooks)
	}
	if n := strings.Count(hooks, "prepare c1 r2"); n != 1 {
		t.Fatalf("r2 prepared %d times, want 1:\n%s", n, hooks)
	}
}

func TestSpecResetInPlaceNeedsHooks(t *testing.T) {
	body := strings.Replace(exampleSpec, "operator:\n  hooks: test\n  revision: 4d2915b3f\n  baseline: base\n  settings: {model: m1, effort: medium}\n", "operator: {builtin: noop}\n", 1)
	if body == exampleSpec {
		t.Fatal("the example spec's operator block moved")
	}
	if _, err := LoadSpec(writeSpec(t, t.TempDir(), body+"reset_in_place: true\n")); err == nil || !strings.Contains(err.Error(), "reset_in_place needs hooks") {
		t.Fatalf("LoadSpec = %v", err)
	}
	if _, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec+"reset_in_place: true\n")); err != nil {
		t.Fatalf("a hooked spec with reset_in_place: %v", err)
	}
}
