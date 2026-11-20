package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

func lanePSI(cpuSome, memFull float64) *Canary {
	return &Canary{IPMillis: 5, LanePSI: map[string]LanePSI{LaneSlice("c1"): {CPUSome: cpuSome, MemFull: memFull}}}
}

// A lane whose own cgroup pressure ("some" cpu avg60, "full" memory avg60) is over its threshold is starved: it gets no
// new build or run and one alarm fires; after three good ticks it is released. The host stays admitted, and what the
// lane already runs is left alone.
func TestStarvedLaneGetsNoNewStepsAndRecoversAfterThreeGoodTicks(t *testing.T) {
	script := []*Canary{lanePSI(10, 0), lanePSI(75, 0), lanePSI(80, 0), lanePSI(10, 0), lanePSI(10, 0), lanePSI(10, 0), lanePSI(0, 30)}
	m := healthManager(t, &script)
	pass := func() {
		t.Helper()
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	alarms := func() (n int) {
		for _, a := range RecentAlarms(m.Root, time.Hour, m.now()) {
			if a.Alarm == "lane-starved" {
				n++
			}
		}
		return
	}
	pass()
	if m.laneGate("c1") != "" {
		t.Fatalf("10%% cpu some: %q", m.laneGate("c1"))
	}
	pass()
	why := m.laneGate("c1")
	if !strings.Contains(why, "unhealthy: starved (cpu") || !strings.Contains(why, "75%") {
		t.Fatalf("cpu some over the threshold must starve the lane and say which: %q", why)
	}
	if m.hostBlock("h1") != "" {
		t.Fatalf("the host stays admitted: %q", m.hostBlock("h1"))
	}
	lf := &LaneFile{Lane: "c1", Host: "h1", State: LaneFree, Prepared: &Prepared{FuzzopsRev: "abc"}}
	m.lanes["c1"] = lf
	if m.start(context.Background(), lf, Step{Kind: StepBuild}) || m.start(context.Background(), lf, Step{Kind: StepRun}) {
		t.Fatal("a starved lane gets no new build or run")
	}
	pass()
	if alarms() != 1 {
		t.Fatalf("one alarm while it stays starved: %d", alarms())
	}
	pass()
	pass()
	if m.laneGate("c1") == "" {
		t.Fatal("two good ticks are not enough")
	}
	// the state survives a manager restart
	script = append(script[:0:0], lanePSI(10, 0))
	m2 := &Manager{Root: m.Root, Ops: m.Ops}
	if err := m2.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m2.laneGate("c1") != "" {
		t.Fatalf("three good ticks across the restart release the lane: %q", m2.laneGate("c1"))
	}
	script = append(script[:0:0], lanePSI(0, 30))
	if err := m2.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if why := m2.laneGate("c1"); !strings.Contains(why, "starved (memory") {
		t.Fatalf("memory full over the threshold starves the lane: %q", why)
	}
}

func TestLanePSIThresholdsComeFromTheInventory(t *testing.T) {
	d := HostHealthConf{}.Thresholds()
	if d.LaneCPUPSISome != 60 || d.LaneMemPSIFull != 20 {
		t.Fatalf("defaults: %+v", d)
	}
	th := (HostHealthConf{LaneCPUPSISome: 30, LaneMemPSIFull: 5}).Thresholds()
	if th.LaneCPUPSISome != 30 || th.LaneMemPSIFull != 5 {
		t.Fatalf("from hosts.yaml: %+v", th)
	}
}
