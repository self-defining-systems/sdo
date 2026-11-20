//go:build integration

package bench

import (
	"context"
	"testing"
	"time"
)

// laneWaitOps reports the steps of the lanes in waiting as being in lane-wait (the build waits for cores).
type laneWaitOps struct {
	fakeOps
	waiting map[string]bool
}

func (o laneWaitOps) Probe(ctx context.Context, steps map[string]string, fz []string) (Probe, error) {
	p, err := o.fakeOps.Probe(ctx, steps, fz)
	p.Phases = map[string]*Phase{}
	for lane := range steps {
		if o.waiting[lane] {
			p.Phases[lane] = &Phase{Name: PhaseLaneWait, Since: time.Now(), Detail: "no free cores"}
		}
	}
	return p, err
}

var _ = context.Background

// A pool slot whose build cannot be placed (it waits for cores) must not keep a lane that already holds cores from
// serving the pool: the free lane is offered before the waiting build, which is cancelled without counting against
// its lane.
func TestFreeLaneWithCoresTakesAPoolSlotFromAWaitingBuild(t *testing.T) {
	l := newFlab(t, "h1=c1,c2,c3") // the pool has 2 slots (2 per host); c3 stays free
	l.specExtra = "hold: true\n"
	l.extra["h1"] = ", max_builds: 3" // room for c3's build once c1's waiting build is gone
	l.writeInventory()
	waiting := map[string]bool{}
	l.m.Ops = func(h string, _ Host) HostOps { return laneWaitOps{fakeOps{l.w, h}, waiting} }
	l.add("pool", []string{"h1"}, "a@x,b@y", 0)
	waiting["c1"] = true // c1's build cannot be placed: it holds no churn slot, so c2 builds too
	l.pass()
	l.advance(time.Second)
	if c1, c2, c3 := l.lane("c1"), l.lane("c2"), l.lane("c3"); c1.State != LaneBuilding || c2.State != LaneBuilding || c3.State != LaneFree {
		t.Fatalf("setup: c1 %s, c2 %s, c3 %s", c1.State, c2.State, c3.State)
	}
	for i := 0; i < 4; i++ {
		l.advance(time.Second)
	}
	if got := l.w.last("c3", StepBuild); got == nil {
		t.Fatal("c3, a free lane that holds cores, never took the pool slot held by c1's waiting build")
	}
	if c1 := l.lane("c1"); c1.BuildFailures != 0 {
		t.Fatalf("the cancelled waiting build counted %d failure(s) against c1", c1.BuildFailures)
	}
}
