//go:build integration && controller_e2e

package e2e

import (
	"testing"
	"time"
)

// The property checks must be able to fail: each case feeds a hand-built log with one defect.
func TestCadenceCheckFlagsStalledAndMissingDetectors(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	specs := []DetectorSpec{{ID: "a", Interval: 2 * time.Second, AddedAt: base.Add(-time.Hour)}, {ID: "b", Interval: 2 * time.Second, AddedAt: base.Add(-time.Hour)}}
	pass := func(seconds int, ids ...string) Iteration {
		return Iteration{Pod: "p", At: base.Add(time.Duration(seconds) * time.Second), Detectors: ids}
	}
	run := func(passes ...Iteration) []string {
		return cadenceViolations(map[string][]Iteration{"p": passes}, []string{"p"}, map[string]bool{"p": false}, specs, nil, nil, base)
	}
	var healthy []Iteration
	for s := 0; s <= 120; s += 2 {
		healthy = append(healthy, pass(s, "a", "b"))
	}
	if v := run(healthy...); len(v) != 0 {
		t.Fatalf("healthy log flagged: %v", v)
	}
	var stalled []Iteration // b stops at 20 s and never returns
	for s := 0; s <= 120; s += 2 {
		if s <= 20 {
			stalled = append(stalled, pass(s, "a", "b"))
		} else {
			stalled = append(stalled, pass(s, "a"))
		}
	}
	if v := run(stalled...); len(v) != 1 {
		t.Fatalf("a detector that stopped must be flagged exactly once, got %v", v)
	}
	var late []Iteration // a gap of 40 s in the middle for a only
	for s := 0; s <= 120; s += 2 {
		if s < 40 || s >= 80 {
			late = append(late, pass(s, "a", "b"))
		} else {
			late = append(late, pass(s, "b"))
		}
	}
	if v := run(late...); len(v) != 1 {
		t.Fatalf("a long gap must be flagged exactly once, got %v", v)
	}
	exempt := map[string]bool{"b": true}
	if v := cadenceViolations(map[string][]Iteration{"p": stalled}, []string{"p"}, map[string]bool{"p": false}, specs, exempt, nil, base); len(v) != 0 {
		t.Fatalf("an exempt detector must not be flagged: %v", v)
	}
	pause := []Window{{From: base.Add(30 * time.Second), To: base.Add(90 * time.Second)}}
	if v := cadenceViolations(map[string][]Iteration{"p": late}, []string{"p"}, map[string]bool{"p": false}, specs, nil, pause, base); len(v) != 0 {
		t.Fatalf("a declared fault window must excuse its gap: %v", v)
	}
	// A new detector must appear after its commit with no restart: activated at 30 s, never listed again.
	added := append([]DetectorSpec(nil), specs...)
	added = append(added, DetectorSpec{ID: "new", Interval: 2 * time.Second, AddedAt: base.Add(30 * time.Second)})
	if v := cadenceViolations(map[string][]Iteration{"p": healthy}, []string{"p"}, map[string]bool{"p": false}, added, nil, nil, base); len(v) == 0 {
		t.Fatal("a detector that never ran after its activation commit must be flagged")
	}
}
