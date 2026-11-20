//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A claimant whose swap fails (its subject, not the lane) does not cost the pool lane: the lane is restored to its
// golden copy in place by the hooks' reset and goes back to the pool, with no teardown, no new cluster and no build
// failure against it. The claim fails with the swap's reason, so the plan's retry/park rule sees an invalid item
// instead of the claim being retried on lane after lane.
func TestBenchFailedSwapRestoresThePoolLaneInPlace(t *testing.T) {
	l := newLab(t)
	l.addReusing("pool", "hold: true\nkeep_lanes: true\n")
	l.until("pool lane held with a golden copy", func() bool {
		lf := l.lane()
		return lf.State == LaneReady && lf.Experiment == "pool" && lf.Step == nil && lf.Prepared != nil && lf.Prepared.Golden
	})
	must(t, os.WriteFile(filepath.Join(l.hooks, "fail-swap"), []byte("error: no ready responder standby within 600s"), 0o644))
	l.addReusing("e2", "claim: pool\nreuse_app: true\n")
	l.until("the claim fails with the swap's reason", func() bool {
		x := l.exp("e2")
		return len(x.AllSeqs()) == 2 && x.Progress(x.AllSeqs()[0]).State == SeqInvalid && x.Progress(x.AllSeqs()[1]).State == SeqInvalid
	})
	x := l.exp("e2")
	if p := x.Progress(x.AllSeqs()[0]); !strings.Contains(p.Reason, "no ready responder standby") {
		t.Fatalf("claim failed with %q, want the swap's reason", p.Reason)
	}
	l.until("the pool lane is back at its golden copy", func() bool {
		lf := l.lane()
		return lf.Step == nil && lf.State == LaneFree && lf.Prepared != nil && lf.Prepared.Golden && lf.Prepared.Reset && lf.Prepared.Ready
	})
	lf := l.lane()
	if lf.BuildFailures != 0 || lf.Recreate || lf.Gone {
		t.Fatalf("a failed swap counted against the lane: %+v", lf)
	}
	hooks := l.read("hooks.log")
	if n := strings.Count(hooks, "swap c1 e2"); n != 2 {
		t.Fatalf("%d swaps, want 2 (one per sequence, none retried):\n%s", n, hooks)
	}
	if n := strings.Count(hooks, "teardown c1 "); n != 1 {
		t.Fatalf("%d teardowns, want only the pool's first build's (the lane is restored, not released):\n%s", n, hooks)
	}
	if !strings.Contains(hooks[strings.Index(hooks, "swap c1 e2"):], "reset c1 ") {
		t.Fatalf("no reset after the failed swap:\n%s", hooks)
	}
}
