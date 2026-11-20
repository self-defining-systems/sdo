package plan

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const alarmPlan = `
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: p1, spec: tpl-plain.yaml}, {name: cold, spec: tpl-plain.yaml}]
items: [a, b]
parallel: 2
stages: {pre-inject: 12m, gate: 5m}
alarms: {idle_lane: 10m, arm_stall: 60m}
`

func mins(m int) time.Duration { return time.Duration(m) * time.Minute }

func TestStageOverTwiceItsMinimumAlarmsOnce(t *testing.T) {
	l := newLab(t)
	e := l.add(alarmPlan)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.exps[exp] = Observation{Phase: PhaseDone, Stages: map[string]time.Duration{"pre-inject": mins(30), "gate": mins(9), "window": mins(99)}}
	l.fb.exps[l.fb.expOf("cold/a/r1")] = Observation{Phase: PhaseDone, Stages: map[string]time.Duration{"pre-inject": mins(24)}}
	l.tick(e)
	// 30 min > 2 x 12; 9 min < 2 x 5 and 24 min is exactly 2 x 12 (not over); "window" has no minimum.
	if got, want := l.fb.kinds(), []string{"stage p1/a/r1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("alarms %v want %v", got, want)
	}
	l.tick(l.open())
	l.tick(l.open())
	if len(l.fb.alarms) != 1 {
		t.Fatalf("alarm repeated: %v", l.fb.kinds())
	}
	if !strings.Contains(l.fb.alarms[0].Detail, "pre-inject") || l.fb.alarms[0].Plan != "tp" {
		t.Fatalf("alarm %+v", l.fb.alarms[0])
	}
}

func TestRunningStageAlarmsWithoutWaitingForItToEnd(t *testing.T) {
	l := newLab(t)
	e := l.add(alarmPlan)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.exps[exp] = Observation{Phase: PhaseRunning, Stage: "gate", StageSince: l.clock.Now().Add(-mins(11))}
	l.tick(e)
	if len(l.fb.alarms) != 1 || l.fb.alarms[0].Alarm != AlarmStage {
		t.Fatalf("alarms %v", l.fb.kinds())
	}
	l.fb.exps[l.fb.expOf("cold/a/r1")] = Observation{Phase: PhaseRunning, Stage: "gate", StageSince: l.clock.Now().Add(-mins(9))}
	l.tick(e)
	if len(l.fb.alarms) != 1 {
		t.Fatalf("a stage under 2x alarmed: %v", l.fb.kinds())
	}
}

func TestFreeLaneIdleWhileWorkWaitsAlarmsAfterTenMinutes(t *testing.T) {
	l := newLab(t)
	e := l.add(alarmPlan)
	free := LaneInfo{Host: "h1", Lane: "c1", Free: true, Since: l.clock.Now().Add(-mins(30))}
	l.fb.capacity = Capacity{Lanes: []LaneInfo{free}, Waiting: 3}
	l.tick(e)
	l.clock.Add(mins(9))
	l.tick(e)
	if len(l.fb.alarms) != 0 {
		t.Fatalf("alarmed before work had waited 10 min: %v", l.fb.kinds())
	}
	l.clock.Add(mins(2))
	l.tick(e)
	if got, want := l.fb.kinds(), []string{"idle-lane h1/c1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("alarms %v want %v", got, want)
	}
	l.clock.Add(mins(5))
	l.tick(l.open())
	if len(l.fb.alarms) != 1 {
		t.Fatalf("repeated while standing: %v", l.fb.kinds())
	}
	// The lane gets work, then idles again: a new alarm.
	l.fb.capacity.Lanes[0].Free = false
	l.tick(e)
	l.fb.capacity.Lanes[0] = LaneInfo{Host: "h1", Lane: "c1", Free: true, Since: l.clock.Now().Add(-mins(11))}
	l.tick(e)
	if len(l.fb.alarms) != 2 {
		t.Fatalf("no second alarm: %v", l.fb.kinds())
	}
}

func TestFreeLaneWithNothingWaitingIsNotAnAlarm(t *testing.T) {
	l := newLab(t)
	e := l.add(`
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: p1, spec: tpl-plain.yaml}]
items: [a]
`)
	l.tick(e) // the one node is released; nothing is ready or queued
	l.fb.capacity = Capacity{Lanes: []LaneInfo{{Host: "h1", Lane: "c1", Free: true, Since: l.clock.Now().Add(-mins(300))}}}
	for i := 0; i < 5; i++ {
		l.clock.Add(mins(10))
		l.tick(e)
	}
	if len(l.fb.alarms) != 0 {
		t.Fatalf("alarms %v", l.fb.kinds())
	}
}

func TestReadyNodesHeldBackByParallelWhileALaneIdlesAlarm(t *testing.T) {
	// parallel 2 with 4 ready nodes and a free lane: the plan itself starves the lane.
	l := newLab(t)
	e := l.add(alarmPlan)
	l.fb.capacity = Capacity{Lanes: []LaneInfo{{Host: "h1", Lane: "c1", Free: true, Since: l.clock.Now().Add(-mins(40))}}}
	l.tick(e)
	l.clock.Add(mins(11))
	l.tick(e)
	if got := l.fb.kinds(); len(got) != 1 || got[0] != "idle-lane h1/c1" {
		t.Fatalf("alarms %v", got)
	}
}

func TestArmWithWorkAndNoProgressAlarms(t *testing.T) {
	l := newLab(t)
	e := l.add(alarmPlan)
	l.tick(e)
	l.clock.Add(mins(59))
	l.tick(e)
	if len(l.fb.alarms) != 0 {
		t.Fatalf("early alarm: %v", l.fb.kinds())
	}
	// cold finishes one run: cold progresses, p1 does not.
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.clock.Add(mins(2))
	l.tick(e)
	got := l.fb.kinds()
	if !reflect.DeepEqual(got, []string{"arm-stall p1"}) {
		t.Fatalf("alarms %v want only p1 stalled", got)
	}
	// p1 finishes: the alarm clears and can fire again later.
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.tick(e)
	if _, ok := l.state().Alarms.Active["arm-stall:p1"]; ok {
		t.Fatal("alarm still standing after progress")
	}
}

func TestHostOverItsBudgetAlarmsAndClears(t *testing.T) {
	l := newLab(t)
	e := l.add(alarmPlan)
	l.fb.capacity = Capacity{Hosts: []HostInfo{{Name: "spokane1", Load5: 120, MaxLoad5: 100}, {Name: "chelan4", Load5: 10, MaxLoad5: 100},
		{Name: "sdovm", Free: 5 << 30, MinFree: 20 << 30}, {Name: "down", Load5: 999, MaxLoad5: 1, Error: "unreachable"}}}
	l.tick(e)
	l.tick(e)
	if got := l.fb.kinds(); !reflect.DeepEqual(got, []string{"host-pressure spokane1", "host-pressure sdovm"}) {
		t.Fatalf("alarms %v", got)
	}
	l.fb.capacity.Hosts[0].Load5 = 50
	l.tick(e)
	l.fb.capacity.Hosts[0].Load5 = 130
	l.tick(e)
	if len(l.fb.alarms) != 3 {
		t.Fatalf("a recovered host that is hot again should alarm again: %v", l.fb.kinds())
	}
}

func TestAlarmConfigIsValidated(t *testing.T) {
	for name, extra := range map[string]string{
		"bad stage":   "stages: {gate: soon}\n",
		"bad factor":  "alarms: {factor: 0.5}\n",
		"bad idle":    "alarms: {idle_lane: -1m}\n",
		"bad timeout": "steps: [{name: m, after: [p1], run: x, timeout: forever}]\n",
	} {
		if _, err := Parse([]byte(retryPlan + extra)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
