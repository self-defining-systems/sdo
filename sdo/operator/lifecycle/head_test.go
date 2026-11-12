package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
)

// headWorld is the advance fake world with a branch head someone else pushed.
type headWorld struct {
	*advWorld
	needed   bool
	recorded []broker.SourceEvidence
	busy     string
	pause    *recordedPause
}

type recordedPause struct {
	events *[]string
	acks   bool
}

func (p *recordedPause) Pause(context.Context, string, time.Time) error {
	*p.events = append(*p.events, "pause")
	return nil
}
func (p *recordedPause) Acknowledged(context.Context) (bool, error) { return p.acks, nil }
func (p *recordedPause) Resume(context.Context) error {
	*p.events = append(*p.events, "resume")
	return nil
}

func (w *headWorld) HeadToDeploy(context.Context) (string, bool, error) {
	w.log("head")
	return advCandidate, w.needed, nil
}

func (w *headWorld) HeadCheckout(_ context.Context, commit string) (string, func(), error) {
	w.log("checkout %s", commit[:4])
	return advCheckout, func() { w.log("release") }, nil
}

func (w *headWorld) RecordHeadDeployment(_ context.Context, commit string, evidence broker.SourceEvidence) error {
	w.log("record %s", commit[:4])
	w.recorded = append(w.recorded, evidence)
	return nil
}

func (w *headWorld) deployer() lifecycle.HeadDeployer {
	repair := lifecycle.RepairDeployer{Readiness: w, Verifier: w, Wait: func(context.Context, time.Duration) error { return nil }, Now: func() time.Time { return time.Unix(1, 0) }}
	if w.pause != nil {
		repair.Maintenance = w.pause
		repair.Busy = func(context.Context) (string, error) { return w.busy, nil }
	}
	return lifecycle.HeadDeployer{Broker: w, Deployer: repair}
}

func newHeadWorld(t *testing.T, needed bool) *headWorld {
	t.Helper()
	return &headWorld{advWorld: newAdvWorld(t), needed: needed}
}

// A head that is already the newest deployment does nothing.
func TestHeadCurrentDeploysNothing(t *testing.T) {
	w := newHeadWorld(t, false)
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadCurrent || !slices.Equal(w.events, []string{"head"}) {
		t.Fatalf("result = %+v, err = %v, events %q", result, err, w.events)
	}
}

// A new head is applied from a clean checkout of that commit, verified by the
// independent verifier, and only then recorded as the newest deployment.
func TestANewHeadIsDeployedVerifiedThenRecorded(t *testing.T) {
	w := newHeadWorld(t, true)
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeployed || result.Commit != advCandidate {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	want := []string{"head", "checkout 4444", "apply", "verify 4444", "record 4444", "release"}
	if !slices.Equal(w.events, want) || len(w.recorded) != 1 || w.recorded[0].EffectiveTree != advEffective {
		t.Fatalf("events\n got %q\nwant %q; recorded %+v", w.events, want, w.recorded)
	}
}

// A head the cluster refuses is held: nothing is recorded.
func TestAHeadTheClusterRefusesIsHeldAndNotRecorded(t *testing.T) {
	w := newHeadWorld(t, true)
	w.applyErrs = []error{errors.New("deployment never became ready")}
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadHeld || result.Hold != lifecycle.HoldRollout || len(w.recorded) != 0 {
		t.Fatalf("result = %+v, err = %v, recorded %d", result, err, len(w.recorded))
	}
}

// With an open incident the deploy never starts: the pause is lifted and the
// head is tried again later.
func TestAHeadIsDeferredWhileAnIncidentIsOpen(t *testing.T) {
	w := newHeadWorld(t, true)
	w.pause, w.busy = &recordedPause{events: &w.events, acks: true}, "an incident is open"
	result, err := w.deployer().Deploy(context.Background(), repairConfig)
	if err != nil || result.Outcome != lifecycle.HeadDeferred || result.Detail != "an incident is open" {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if slices.Contains(w.events, "apply") || w.events[len(w.events)-1] != "resume" {
		t.Fatalf("events %q: nothing may be applied and the pause must be lifted", w.events)
	}
}
