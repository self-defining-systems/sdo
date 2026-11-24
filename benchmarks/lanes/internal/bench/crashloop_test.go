package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCrashTrackerCountsRestartsInTheWindowOnly(t *testing.T) {
	th := CrashLoopConf{}.Thresholds()
	if th.Restarts != 10 || th.Window != 10*time.Minute {
		t.Fatalf("defaults: %+v", th)
	}
	t0 := time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC)
	var tr crashTracker
	if n := tr.Observe(t0, map[string]int64{"ns/a/c": 40}, th.Window); n != 0 {
		t.Fatalf("restarts that happened before the first look are history, not a loop: %d", n)
	}
	if n := tr.Observe(t0.Add(time.Minute), map[string]int64{"ns/a/c": 47}, th.Window); n != 7 {
		t.Fatalf("7 restarts in a minute: %d", n)
	}
	if n := tr.Observe(t0.Add(2*time.Minute), map[string]int64{"ns/a/c": 47, "ns/new/c": 4}, th.Window); n != 11 {
		t.Fatalf("a pod that appeared with 4 restarts adds them: %d", n)
	}
	// the burst ages out of the window
	if n := tr.Observe(t0.Add(30*time.Minute), map[string]int64{"ns/a/c": 47, "ns/new/c": 4}, th.Window); n != 0 {
		t.Fatalf("old restarts leave the window: %d", n)
	}
	// a pod that was deleted and recreated counts the restarts it has since its own start, never a negative number
	tr.Observe(t0.Add(31*time.Minute), map[string]int64{"ns/a/c": 2}, th.Window)
	if n := tr.Observe(t0.Add(32*time.Minute), map[string]int64{"ns/a/c": 3}, th.Window); n != 3 {
		t.Fatalf("a recreated pod counts from its own start: %d", n)
	}
}

// podOps answers the manager's one kubectl read per lane per tick from a script.
type podOps struct {
	HostOps
	pods  *map[string]map[string]int64
	reads *int
}

func (p podOps) Probe(context.Context, map[string]string, []string) (Probe, error) {
	return Probe{Steps: map[string]string{}}, nil
}

func (p podOps) PodRestarts(_ context.Context, lane, _ string) (map[string]int64, error) {
	*p.reads++
	return (*p.pods)[lane], nil
}

// A lane whose pods restart more than K times in M minutes is marked unhealthy: it gets no new build or run, an alarm
// fires once, and nothing running is touched. It is released as the restarts age out of the window.
func TestCrashLoopingLaneGetsNoNewSteps(t *testing.T) {
	pods := map[string]map[string]int64{"c1": {"ns/a/c": 0}}
	reads := 0
	root := Root(t.TempDir())
	inv := "hosts:\n  h1: {local: true, staging: /tmp/s, max_builds: 1, max_load5: 100, kubeconfigs: /tmp/k, evidence: /tmp/e, clusters: [c1]}\n" +
		"checkouts:\n  fuzzops: {abc: {h1: /tmp/fz}}\n"
	if err := os.WriteFile(filepath.Join(string(root), "hosts.yaml"), []byte(inv), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC)
	var started []Step
	m := &Manager{Root: root, Now: func() time.Time { return now }}
	m.Ops = func(string, Host) HostOps { return podStartOps{podOps{pods: &pods, reads: &reads}, &started} }
	pass := func(dt time.Duration) {
		t.Helper()
		now = now.Add(dt)
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	alarms := func() int {
		n := 0
		for _, a := range RecentAlarms(root, time.Hour, now) {
			if a.Alarm == "lane-crash-looping" {
				n++
			}
		}
		return n
	}
	pass(0)
	pods["c1"] = map[string]int64{"ns/a/c": 12}
	pass(time.Minute)
	lane := m.lanes["c1"]
	lane.State = LaneReady
	lane.Prepared = &Prepared{Ready: true, WorkloadRev: "abc"}
	if err := writeJSON(root.LanePath("c1"), lane); err != nil {
		t.Fatal(err)
	}
	pods["c1"] = map[string]int64{"ns/a/c": 24}
	pass(time.Minute)
	if why := m.crashBlock("c1"); why != "" || alarms() != 0 || reads != 1 {
		t.Fatalf("first ready sample baselines bring-up restarts: block=%q alarms=%d reads=%d", why, alarms(), reads)
	}
	// A replacement can start and finish between ticks. The next ready sample starts a new baseline.
	lane = m.lanes["c1"]
	if !m.start(context.Background(), lane, Step{Kind: StepBuild}) {
		t.Fatal("bring-up restarts must not hold back a replacement build")
	}
	lane.Step = nil
	if err := writeJSON(root.LanePath("c1"), lane); err != nil {
		t.Fatal(err)
	}
	pods["c1"] = map[string]int64{"ns/replacement/c": 12}
	pass(time.Minute)
	if why := m.crashBlock("c1"); why != "" || alarms() != 0 {
		t.Fatalf("replacement bring-up restarts must not count: block=%q alarms=%d", why, alarms())
	}
	pods["c1"] = map[string]int64{"ns/replacement/c": 24}
	pass(time.Minute)
	why := m.crashBlock("c1")
	if !strings.Contains(why, "crash-looping") || !strings.Contains(why, "12") {
		t.Fatalf("12 restarts in a minute: %q", why)
	}
	pass(time.Minute)
	if alarms() != 1 {
		t.Fatalf("one alarm while it stays marked: %d", alarms())
	}
	lf := &LaneFile{Lane: "c1", Host: "h1", State: LaneFree, Prepared: &Prepared{WorkloadRev: "abc"}}
	m.lanes["c1"] = lf
	if m.start(context.Background(), lf, Step{Kind: StepBuild}) || m.start(context.Background(), lf, Step{Kind: StepRun}) {
		t.Fatal("a crash-looping lane gets no new build or run")
	}
	if !m.start(context.Background(), lf, Step{Kind: StepRecover}) {
		t.Fatal("cleanup of what the lane already holds is not held back")
	}
	if !strings.Contains(lf.Reason+m.crashBlock("c1"), "crash-looping") {
		t.Fatal("the reason is visible")
	}
	pass(20 * time.Minute)
	if m.crashBlock("c1") != "" {
		t.Fatalf("restarts aged out of the window, the lane is released: %q", m.crashBlock("c1"))
	}
}

type podStartOps struct {
	podOps
	started *[]Step
}

func (p podStartOps) Start(_ context.Context, s Step) error {
	*p.started = append(*p.started, s)
	return nil
}
