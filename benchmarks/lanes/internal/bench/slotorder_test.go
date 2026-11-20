//go:build integration

package bench

import (
	"context"
	"testing"
	"time"
)

// waitingOps reports every step as running; steps whose lane is in waiting are in lane-wait.
type waitingOps struct {
	churnOps
	waiting map[string]bool
}

func (w waitingOps) Probe(ctx context.Context, steps map[string]string, l []string) (Probe, error) {
	p, err := w.churnOps.Probe(ctx, steps, l)
	p.Phases = map[string]*Phase{}
	for lane := range steps {
		if w.waiting[lane] {
			p.Phases[lane] = &Phase{Name: PhaseLaneWait, Since: time.Now(), Detail: "no free cores"}
		}
	}
	return p, err
}

// LB15: a build takes its churn slot only after its cores are placed. With 2 slots, 2 builds waiting for cores and
// 2 lanes that own cores and need a rebuild (release/reset), the waiters used to hold both slots, so the owners could
// never rebuild and never freed the cores the waiters wait for: a deadlock.
func TestBuildsWaitingForCoresHoldNoChurnSlot(t *testing.T) {
	var started []Step
	m := churnManager(t, 2, &started)
	m.Ops = func(string, Host) HostOps {
		return waitingOps{churnOps: churnOps{started: &started}, waiting: map[string]bool{"c1": true, "c2": true}}
	}
	prepared := &Prepared{Experiment: "x", Hooks: "hk", WorkloadRev: "abc"}
	writeLane(t, m, LaneFile{Lane: "c1", Host: "h1", State: LaneBuilding, Step: &StepRef{ID: "build-1", Kind: StepBuild}})
	writeLane(t, m, LaneFile{Lane: "c2", Host: "h1", State: LaneBuilding, Step: &StepRef{ID: "build-2", Kind: StepBuild}})
	writeLane(t, m, LaneFile{Lane: "c3", Host: "h1", State: LaneFree, Prepared: prepared})
	writeLane(t, m, LaneFile{Lane: "c4", Host: "h1", State: LaneFree, Prepared: prepared})
	for i := 0; i < 4; i++ {
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := releases(started); n != 2 {
		t.Fatalf("2 builds wait for cores and hold no slot, so both lanes that own cores must be able to rebuild; got %d release(s)", n)
	}
}

// A build that is past lane-wait does hold its slot.
func TestBuildPastLaneWaitHoldsItsChurnSlot(t *testing.T) {
	var started []Step
	m := churnManager(t, 1, &started)
	m.Ops = func(string, Host) HostOps {
		return waitingOps{churnOps: churnOps{started: &started}, waiting: map[string]bool{}}
	}
	prepared := &Prepared{Experiment: "x", Hooks: "hk", WorkloadRev: "abc"}
	writeLane(t, m, LaneFile{Lane: "c1", Host: "h1", State: LaneBuilding, Step: &StepRef{ID: "build-1", Kind: StepBuild}})
	writeLane(t, m, LaneFile{Lane: "c3", Host: "h1", State: LaneFree, Prepared: prepared})
	if err := m.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := releases(started); n != 0 {
		t.Fatalf("a placed build holds the only slot, got %d release(s)", n)
	}
}
