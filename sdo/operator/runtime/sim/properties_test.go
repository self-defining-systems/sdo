package sim_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
)

// Each check has hand-built logs: a violating log shows it can fail and a
// correct one that it passes. A "controller mutant" note names the deliberate
// controller bug the harness must catch.

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

var crash = sim.FindingRef{Key: "pods\x00crash-api", Detector: "pods"}

func wantViolation(t *testing.T, err error, property, contains string) {
	t.Helper()
	var v *sim.Violation
	if !errors.As(err, &v) {
		t.Fatalf("want a %s violation, got %v", property, err)
	}
	if v.Property != property || !strings.Contains(v.Error(), contains) {
		t.Fatalf("want %s violation mentioning %q, got %v", property, contains, err)
	}
}

func wantOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected violation: %v", err)
	}
}

func boundedRun(entries ...sim.Entry) sim.Log {
	return append(sim.Log{sim.FaultsQuiesced{At: at(100)}}, append(entries, sim.RunEnded{At: at(400)})...)
}

// Reliability. Controller mutant: when a closure fails permanently, the
// controller keeps the failed incident as the current one and never opens or
// dispatches the next, so later incidents stay open to the end of the run.
func TestEveryIncidentEnds(t *testing.T) {
	cfg := sim.Config{Settle: 200 * time.Second}
	open := func(id string, s int) sim.Opened {
		return sim.Opened{At: at(s), Incident: id, Findings: []sim.FindingRef{crash}}
	}
	good := boundedRun(
		open("i1", 10), sim.Outcome{At: at(50), Incident: "i1", Result: sim.Failed},
		open("i2", 60), sim.Outcome{At: at(90), Incident: "i2", Result: sim.Closed},
	)
	wantOK(t, sim.EveryIncidentEnds(good, cfg))

	t.Run("never ends", func(t *testing.T) {
		wantViolation(t, sim.EveryIncidentEnds(boundedRun(open("i1", 10)), cfg), sim.PropEveryIncidentEnds, `incident "i1"`)
	})
	t.Run("failed incident blocks the next", func(t *testing.T) {
		log := boundedRun(open("i1", 10), sim.Outcome{At: at(50), Incident: "i1", Result: sim.Failed}, open("i2", 60))
		err := sim.EveryIncidentEnds(log, cfg)
		wantViolation(t, err, sim.PropEveryIncidentEnds, `blocked behind failed incident "i1"`)
	})
	t.Run("outcome after the end of the run does not count", func(t *testing.T) {
		log := boundedRun(open("i1", 10), sim.Outcome{At: at(500), Incident: "i1", Result: sim.Closed})
		wantViolation(t, sim.EveryIncidentEnds(log, cfg), sim.PropEveryIncidentEnds, "no terminal outcome")
	})
	t.Run("conflicting outcomes", func(t *testing.T) {
		log := boundedRun(open("i1", 10), sim.Outcome{At: at(50), Incident: "i1", Result: sim.Closed}, sim.Outcome{At: at(60), Incident: "i1", Result: sim.Failed})
		wantViolation(t, sim.EveryIncidentEnds(log, cfg), sim.PropEveryIncidentEnds, "outcomes")
	})
	t.Run("a controller that keeps dying after the faults stop is wedged", func(t *testing.T) {
		// Controller mutant: state restore fails on every start, so the process
		// exits and is relaunched forever; each relaunch looks like a restart.
		loop := cfg
		loop.Restart = 30 * time.Second
		restarting := boundedRun(sim.ControllerDown{At: at(110)}, sim.ControllerUp{At: at(112)})
		wantOK(t, sim.EveryIncidentEnds(restarting, loop))
		wedged := boundedRun(sim.ControllerDown{At: at(110)}, sim.ControllerUp{At: at(112)}, sim.ControllerDown{At: at(300)}, sim.ControllerUp{At: at(301)})
		wantViolation(t, sim.EveryIncidentEnds(wedged, loop), sim.PropEveryIncidentEnds, "keeps failing without faults")
	})
	t.Run("a run without bounded faults is rejected, not passed", func(t *testing.T) {
		wantViolation(t, sim.EveryIncidentEnds(sim.Log{open("i1", 10), sim.Outcome{At: at(20), Incident: "i1"}}, cfg), sim.PropEveryIncidentEnds, "not shown to be bounded")
		short := sim.Config{Settle: time.Hour}
		wantViolation(t, sim.EveryIncidentEnds(good, short), sim.PropEveryIncidentEnds, "settle time")
	})
}

// Detection. Controller mutant: memory activation holds the evaluation lock
// while it compiles the new revision (the sim's compile delay), a swap drops a
// detector's schedule, or closure waits on the agent inside the evaluation loop,
// so no detector runs for the duration.
func TestDetectionKeepsRunning(t *testing.T) {
	cfg := sim.Config{GapFactor: 3, Slack: 2 * time.Second}
	fast := sim.DetectorDecl{ID: "fast", Interval: 5 * time.Second}  // bound 17s
	slow := sim.DetectorDecl{ID: "slow", Interval: 20 * time.Second} // bound 62s
	evals := func(detector string, failed bool, secs ...int) (out []sim.Entry) {
		for _, s := range secs {
			out = append(out, sim.Evaluated{At: at(s), Detector: detector, Failed: failed})
		}
		return out
	}
	build := func(parts ...[]sim.Entry) sim.Log {
		log := sim.Log{sim.Activated{At: at(0), Revision: "r1", Detectors: []sim.DetectorDecl{fast, slow}}}
		for _, p := range parts {
			log = append(log, p...)
		}
		return append(log, sim.RunEnded{At: at(100)})
	}
	slowOK := evals("slow", false, 30, 70, 99)
	fastOK := evals("fast", false, 10, 25, 40, 55, 70, 85, 99)
	wantOK(t, sim.DetectionKeepsRunning(build(fastOK, slowOK), cfg))

	t.Run("each detector has its own bound", func(t *testing.T) {
		// A 40 s gap is fine for the slow detector and a violation for the fast one.
		wantOK(t, sim.DetectionKeepsRunning(build(fastOK, evals("slow", false, 40, 80)), cfg))
		wantViolation(t, sim.DetectionKeepsRunning(build(evals("fast", false, 10, 50, 90), slowOK), cfg), sim.PropDetectionRuns, `"fast" was not evaluated for 40s (limit 17s`)
	})
	t.Run("gap in the middle", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(build(fastOK, evals("slow", false, 10, 80, 99)), cfg), sim.PropDetectionRuns, `"slow" was not evaluated for 1m10s`)
	})
	t.Run("detector never evaluated", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(build(fastOK), cfg), sim.PropDetectionRuns, `"slow"`)
	})
	t.Run("silent after the last evaluation", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(build(slowOK, evals("fast", false, 10, 25)), cfg), sim.PropDetectionRuns, `"fast"`)
	})
	t.Run("a failing detector still counts as scheduled", func(t *testing.T) {
		wantOK(t, sim.DetectionKeepsRunning(build(slowOK, evals("fast", true, 10, 25, 40, 55, 70, 85, 99)), cfg))
	})
	t.Run("a hung detector is exempt but delays no other", func(t *testing.T) {
		hung := cfg
		hung.Hung = map[string]bool{"fast": true}
		wantOK(t, sim.DetectionKeepsRunning(build(slowOK), hung))
		wantViolation(t, sim.DetectionKeepsRunning(build(evals("slow", false, 10, 90)), hung), sim.PropDetectionRuns, `"slow"`)
	})
	t.Run("controller downtime is not counted", func(t *testing.T) {
		log := build(evals("slow", false, 30), evals("fast", false, 10, 20), []sim.Entry{sim.ControllerDown{At: at(21)}, sim.ControllerUp{At: at(80)}}, evals("fast", false, 90, 99), evals("slow", false, 90))
		wantOK(t, sim.DetectionKeepsRunning(log, cfg))
	})
	t.Run("a new revision's detector must start on time, a retired one must stop", func(t *testing.T) {
		newer := sim.DetectorDecl{ID: "new", Interval: 10 * time.Second} // bound 32s
		swap := sim.Log{
			sim.Activated{At: at(0), Revision: "r1", Detectors: []sim.DetectorDecl{fast}},
			sim.Evaluated{At: at(10), Detector: "fast"}, sim.Evaluated{At: at(20), Detector: "fast"},
			sim.Activated{At: at(25), Revision: "r2", Detectors: []sim.DetectorDecl{newer}},
		}
		with := func(extra ...sim.Entry) sim.Log {
			return append(append(sim.Log(nil), swap...), append(extra, sim.RunEnded{At: at(100)})...)
		}
		wantViolation(t, sim.DetectionKeepsRunning(with(), cfg), sim.PropDetectionRuns, `"new"`)
		onTime := evals("new", false, 40, 70, 99)
		wantOK(t, sim.DetectionKeepsRunning(with(onTime...), cfg))
		stale := append(evals("fast", false, 60), onTime...)
		wantViolation(t, sim.DetectionKeepsRunning(with(stale...), cfg), sim.PropDetectionRuns, `"fast" was removed`)
	})
	t.Run("evaluation before any activation", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(sim.Log{sim.Evaluated{At: at(1), Detector: "fast"}, sim.RunEnded{At: at(2)}}, cfg), sim.PropDetectionRuns, "before any memory revision")
	})
}

func TestCheckReportsEveryViolation(t *testing.T) {
	log := sim.Log{sim.Activated{At: at(0), Detectors: []sim.DetectorDecl{{ID: "d", Interval: time.Second}}}, sim.Opened{At: at(1), Incident: "i"}, sim.RunEnded{At: at(100)}}
	if got := sim.Check(log, sim.Config{GapFactor: 2, Settle: time.Second}); len(got) != 2 {
		t.Fatalf("want a violation of each property, got %v", got)
	}
}

// A detector's evaluation is recorded whether it returns, errors or panics.
func TestRecorderDetectorRecordsFailuresAndPanics(t *testing.T) {
	rec := sim.NewRecorder(func() time.Time { return t0 })
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		det    sdk.Detector
		failed bool
	}{
		{"ok", stubDetector{id: "ok"}, false},
		{"err", stubDetector{id: "err", err: errors.New("boom")}, true},
		{"panic", stubDetector{id: "panic", panics: true}, true},
	} {
		func() {
			defer func() { _ = recover() }()
			_, _ = rec.Detector(tc.det).Detect(ctx, nil)
		}()
		last := rec.Log()[len(rec.Log())-1].(sim.Evaluated)
		if last.Detector != tc.name || last.Failed != tc.failed {
			t.Fatalf("%s: recorded %#v", tc.name, last)
		}
	}
}

type stubDetector struct {
	id     string
	err    error
	panics bool
}

func (d stubDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id} }
func (d stubDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.panics {
		panic("detector panic")
	}
	return nil, d.err
}

// The recorder's seams: Kube.Patch records writes with their attribution and
// Cache.Snapshot records what each read reflects, including a stale one.
func TestRecorderCapturesWritesAndStaleObservations(t *testing.T) {
	sim.Run(t, 3, func(t *testing.T, w *sim.World) {
		rec := sim.NewRecorder(w.Now)
		w.Kube.Recorder = rec
		cm := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "cfg"}}}
		w.Kube.Put(cm)
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "cfg"}
		ctx := sim.WithAttribution(context.Background(), sim.Attribution{Incident: "i1", Binding: "b1", Attempt: 1})

		w.Cache.SetFaults(sim.Faults{ChurnPerPatch: func() int { return 0 }, FailAfterCommit: func() bool { return false }, StaleReads: func() int { return 1 }})
		if err := w.Kube.Patch(ctx, ref, types.MergePatchType, []byte(`{"data":{"a":"1"}}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Cache.Snapshot(ctx); err != nil { // stale: still shows the pre-write cluster
			t.Fatal(err)
		}
		if _, err := w.Cache.Snapshot(ctx); err != nil { // fresh
			t.Fatal(err)
		}
		log := rec.Log()
		if len(log) != 3 {
			t.Fatalf("want write and two observations, got %#v", log)
		}
		wr, ok := log[0].(sim.Write)
		if !ok || !wr.Committed || wr.Seq != 1 || wr.Object != ref || wr.Attribution.Binding != "b1" || wr.Patch == "" {
			t.Fatalf("write: %#v", log[0])
		}
		stale, fresh := log[1].(sim.Observation), log[2].(sim.Observation)
		if stale.ThroughSeq != 0 || fresh.ThroughSeq != 1 {
			t.Fatalf("stale read must lag the write: stale=%d fresh=%d", stale.ThroughSeq, fresh.ThroughSeq)
		}
		// A stale read is visible to the harness as a view older than the latest write.
	})
}

// The recorder implements the controller's existing lifecycle sink
// interface, deduplicated by event id as the durable sinks are.
func TestRecorderSinks(t *testing.T) {
	rec := sim.NewRecorder(func() time.Time { return t0 })
	ev := func(id, kind string, dispatchError bool) *contracts.LifecycleEvent {
		return &contracts.LifecycleEvent{EventId: id + kind, Event: kind, IncidentId: id, RecordedAt: timestamppb.New(t0), DispatchError: dispatchError, FindingKeys: []string{"pods\x00crash"}}
	}
	for _, e := range []*contracts.LifecycleEvent{
		ev("i1", "incident_opened", false), ev("i1", "incident_opened", false), ev("i1", "responder_dispatched", false),
		ev("i1", "incident_closed", true), ev("i1", "incident_acknowledged", false),
		ev("i2", "incident_opened", false), ev("i2", "incident_acknowledged", false),
	} {
		if err := rec.RecordLifecycle(e); err != nil {
			t.Fatal(err)
		}
	}
	var kinds []string
	for _, e := range rec.Log() {
		switch x := e.(type) {
		case sim.Opened:
			kinds = append(kinds, "opened:"+x.Incident+":"+x.Findings[0].Detector)
		case sim.Handler:
			kinds = append(kinds, "handler:"+string(x.Kind))
		case sim.Outcome:
			kinds = append(kinds, "outcome:"+x.Incident+":"+string(x.Result))
		}
	}
	want := "opened:i1:pods handler:agent outcome:i1:failed opened:i2:pods outcome:i2:closed"
	if got := strings.Join(kinds, " "); got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}
