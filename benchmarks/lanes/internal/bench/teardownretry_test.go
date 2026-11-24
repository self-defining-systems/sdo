package bench

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

type teardownOps struct {
	HostOps
	kills   int
	started []Step
	phase   string
	result  Result
}

func (o *teardownOps) Kill(context.Context, string, string) error {
	o.kills++
	return nil
}

func (o *teardownOps) Start(_ context.Context, s Step) error {
	o.started = append(o.started, s)
	return nil
}

func (o *teardownOps) Fetch(_ context.Context, _, _, dest string) error {
	if err := writeJSON(filepath.Join(dest, phaseFile), Phase{Name: o.phase}); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dest, "result.json"), o.result)
}

func (*teardownOps) Clean(context.Context, string, string) error { return nil }

func TestRemovedBuildTeardown(t *testing.T) {
	for _, tc := range []struct {
		name, phase string
		dead        bool
		release     bool
		exit        int
	}{
		{name: "delete still running", phase: "cluster-down"},
		{name: "operator teardown still running", phase: "teardown"},
		{name: "signal after probe", phase: "cluster-down", exit: -1},
		{name: "runner died", phase: "cluster-down", dead: true},
		{name: "release died before its first phase", dead: true, release: true},
		{name: "ordinary failure", phase: "cluster-down", exit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			o := &teardownOps{phase: tc.phase, result: Result{Exit: tc.exit, LaneDown: "delete failed"}}
			m := Manager{
				Root: Root(t.TempDir()),
				Now:  func() time.Time { return time.Unix(0, 0) },
				Ops:  func(string, Host) HostOps { return o },
				inv:  Inventory{Hosts: map[string]Host{"host": {ClusterDown: "delete"}}},
				exps: map[string]*Experiment{"pool": {Removed: &Removal{}}},
			}
			l := LaneFile{Lane: "lane", Host: "host", Experiment: "pool", State: LaneBuilding,
				Step: &StepRef{ID: "build-1", Kind: StepBuild, Phase: &Phase{Name: tc.phase}}}
			if tc.release {
				l.Step.Kind, l.Step.DeleteCluster = StepRelease, true
			}
			switch {
			case tc.dead:
				m.died(ctx, &l)
			case tc.exit != 0:
				l.Step.Phase = nil // the phase changed since the last probe
				if err := m.adopt(ctx, &l); err != nil {
					t.Fatal(err)
				}
			default:
				m.cancelRemoved(ctx, &l)
				if o.kills != 0 {
					t.Fatal("removal cancelled active teardown")
				}
				return
			}
			if tc.exit == 1 {
				if l.State != LaneDown || len(o.started) != 0 {
					t.Fatalf("ordinary delete failure lost: %+v", l)
				}
				return
			}
			if l.State != LaneFree || l.BuildFailures != 0 || len(o.started) != 1 || !o.started[0].DeleteCluster {
				t.Fatalf("interrupted delete did not restart: lane=%+v steps=%+v", l, o.started)
			}
		})
	}
}
