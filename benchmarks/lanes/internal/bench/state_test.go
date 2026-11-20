//go:build integration

package bench

import "testing"

func TestProgressIsDerivedFromRunFiles(t *testing.T) {
	spec := Spec{Faults: Faults{IDs: []string{"a@x"}}, Sequences: [][]int{{1, 2, 2}}, Retries: 1}
	q := Seq{"a@x", 1}
	run := func(pos, attempt int, status string) RunFile {
		return RunFile{ID: RunID("a@x", 1, pos, attempt), Fault: "a@x", Sequence: 1, Position: pos, Attempt: attempt, Status: status, Reason: "capped: busy"}
	}
	for _, c := range []struct {
		name    string
		runs    []RunFile
		removed bool
		want    Progress
	}{
		{"nothing ran", nil, false, Progress{Seq: q, State: SeqQueued, Attempt: 1, Next: 1}},
		{"first done", []RunFile{run(1, 1, StatusDone)}, false, Progress{Seq: q, State: SeqStarted, Attempt: 1, Next: 2}},
		{"all done", []RunFile{run(1, 1, StatusDone), run(2, 1, StatusDone), run(3, 1, StatusDone)}, false, Progress{Seq: q, State: SeqDone, Attempt: 1}},
		{"broken, retry left", []RunFile{run(1, 1, StatusDone), run(2, 1, StatusInvalid)}, false, Progress{Seq: q, State: SeqQueued, Attempt: 2, Next: 1}},
		{"broken twice", []RunFile{run(1, 1, StatusInvalid), run(1, 2, StatusInvalid)}, false, Progress{Seq: q, State: SeqInvalid, Attempt: 2, Reason: "capped: busy"}},
		{"retry under way", []RunFile{run(1, 1, StatusInvalid), run(1, 2, StatusDone)}, false, Progress{Seq: q, State: SeqStarted, Attempt: 2, Next: 2}},
		{"fault removed", []RunFile{run(1, 1, StatusDone)}, true, Progress{Seq: q, State: SeqDropped, Attempt: 1}},
	} {
		s := spec
		if c.removed {
			s.Faults.IDs = []string{"b@y"}
		}
		x := &Experiment{Spec: &s, Lock: Lock{Spec: spec}, Runs: map[string]RunFile{}}
		for _, r := range c.runs {
			x.Runs[r.ID] = r
		}
		if got := x.Progress(q); got != c.want {
			t.Errorf("%s: got %+v want %+v", c.name, got, c.want)
		}
	}
}

func TestCountsQueuedAndDropped(t *testing.T) {
	spec := Spec{Faults: Faults{IDs: []string{"a@x", "b@y"}}, Sequences: [][]int{{1, 2}}}
	cur := spec
	cur.Faults.IDs = []string{"a@x"}
	x := &Experiment{Spec: &cur, Lock: Lock{Spec: spec}, Runs: map[string]RunFile{}}
	r := RunFile{ID: "r", Fault: "a@x", Sequence: 1, Position: 1, Attempt: 1, Status: StatusDone}
	x.Runs[r.ID] = r
	c := x.Count(0)
	if c.Done != 1 || c.Queued != 1 || c.Dropped != 2 {
		t.Fatalf("%+v", c)
	}
}

// An edit refused for its identity still applies every live field (keep_lanes, reuse_app, ...), not a hand-picked few.
func TestRefusedIdentityEditKeepsEveryLiveField(t *testing.T) {
	lock, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec))
	if err != nil {
		t.Fatal(err)
	}
	edited := lock
	edited.Cap, edited.Namespace = "99m", "elsewhere"                                                 // identity: refused
	edited.KeepLanes, edited.ReuseApp, edited.Lanes, edited.Faults.IDs = true, true, 7, []string{"x"} // live
	got := edited.withIdentityOf(lock)
	if d := got.IdentityDiff(Lock{Spec: lock}); len(d) != 0 {
		t.Fatalf("identity not restored: %v", d)
	}
	if !got.KeepLanes || !got.ReuseApp || got.Lanes != 7 || len(got.Faults.IDs) != 1 || got.Faults.IDs[0] != "x" {
		t.Fatalf("live fields lost: %+v", got)
	}
}
