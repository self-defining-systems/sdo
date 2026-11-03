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

// Each property has two kinds of test here. A hand-built log that violates it
// shows the check can fail (and that a correct log passes). A "controller
// mutant" note names the deliberate controller bug that the check must catch
// once the step-2 harness records a real run; no mutant is built in this step,
// because the harness that runs the controller does not exist yet.

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

var (
	deploy  = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	service = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	crash   = sim.FindingRef{Key: "pods\x00crash-api", Detector: "pods"}
	lateF   = sim.FindingRef{Key: "pods\x00late-db", Detector: "pods"}
)

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

// Property 1. Controller mutant: after a patch, the stage marks the finding
// fixed from the previous verification (or from a stale informer snapshot)
// instead of taking a new observation; the stale-cache fault makes the mark
// precede the write the view reflects.
func TestFixedMeansVerified(t *testing.T) {
	fixed := func(obs int) sim.Fixed {
		return sim.Fixed{At: at(5), Incident: "i1", Finding: crash, Scope: []sdk.ObjectRef{deploy}, Observation: obs}
	}
	good := sim.Log{
		sim.Write{At: at(1), Seq: 1, Object: deploy, Committed: true},
		sim.Observation{At: at(2), ID: 1, ThroughSeq: 1},
		sim.Evaluated{At: at(2), Detector: "pods", Observation: 1},
		fixed(1),
	}
	wantOK(t, sim.FixedMeansVerified(good))

	t.Run("observation predates the write", func(t *testing.T) {
		log := sim.Log{
			sim.Observation{At: at(0), ID: 1, ThroughSeq: 0},
			sim.Evaluated{At: at(0), Detector: "pods", Observation: 1},
			sim.Write{At: at(1), Seq: 1, Object: deploy, Committed: true},
			fixed(1),
		}
		wantViolation(t, sim.FixedMeansVerified(log), sim.PropFixedMeansVerified, "wrote Deployment/app/api")
	})
	t.Run("stale cache hides a write that happened before the observation", func(t *testing.T) {
		log := sim.Log{
			sim.Write{At: at(1), Seq: 1, Object: deploy, Committed: true},
			sim.Observation{At: at(2), ID: 1, ThroughSeq: 0},
			sim.Evaluated{At: at(2), Detector: "pods", Observation: 1},
			fixed(1),
		}
		wantViolation(t, sim.FixedMeansVerified(log), sim.PropFixedMeansVerified, "write 1")
	})
	t.Run("an unrelated write does not matter, an unknown scope matches every write", func(t *testing.T) {
		unrelated := sim.Log{
			sim.Observation{At: at(0), ID: 1},
			sim.Evaluated{At: at(0), Detector: "pods", Observation: 1},
			sim.Write{At: at(1), Seq: 1, Object: service, Committed: true},
			fixed(1),
		}
		wantOK(t, sim.FixedMeansVerified(unrelated))
		unknown := append(sim.Log(nil), unrelated...)
		unknown[3] = sim.Fixed{At: at(5), Incident: "i1", Finding: crash, Observation: 1}
		wantViolation(t, sim.FixedMeansVerified(unknown), sim.PropFixedMeansVerified, "wrote Service")
	})
	t.Run("a write that did not reach the cluster does not matter", func(t *testing.T) {
		log := sim.Log{
			sim.Observation{At: at(0), ID: 1},
			sim.Evaluated{At: at(0), Detector: "pods", Observation: 1},
			sim.Write{At: at(1), Object: deploy, Committed: false},
			fixed(1),
		}
		wantOK(t, sim.FixedMeansVerified(log))
	})
	t.Run("the observation still showed the finding", func(t *testing.T) {
		log := sim.Log{
			sim.Observation{At: at(0), ID: 1},
			sim.Evaluated{At: at(0), Detector: "pods", Observation: 1, Active: []string{crash.Key}},
			fixed(1),
		}
		wantViolation(t, sim.FixedMeansVerified(log), sim.PropFixedMeansVerified, "still showed it active")
	})
	t.Run("no observation or no evaluation backs the mark", func(t *testing.T) {
		wantViolation(t, sim.FixedMeansVerified(sim.Log{fixed(7)}), sim.PropFixedMeansVerified, "observation 7")
		log := sim.Log{sim.Observation{At: at(0), ID: 1}, fixed(1)}
		wantViolation(t, sim.FixedMeansVerified(log), sim.PropFixedMeansVerified, "no evaluation")
	})
}

// Property 2. Controller mutant: the deterministic stage persists "applied"
// only after the patch returns, so a crash (or the sim's commit-then-timeout
// fault) between the patch and the save makes the restarted controller replay
// the same attempt for the same binding.
func TestWriteAtMostOnce(t *testing.T) {
	attr := func(attempt int) sim.Attribution {
		return sim.Attribution{Incident: "i1", Binding: "scale-api{api=Deployment/api}", Attempt: attempt}
	}
	write := func(s int, attempt int, patch string, committed bool) sim.Write {
		return sim.Write{At: at(s), Attribution: attr(attempt), Object: deploy, Patch: patch, Committed: committed}
	}
	good := sim.Log{write(1, 1, "p1", false), write(2, 2, "p1", true), write(3, 1, "p2", true)}
	wantOK(t, sim.WriteAtMostOnce(good))

	t.Run("replayed after a lost reply", func(t *testing.T) {
		log := sim.Log{write(1, 1, "p1", true), write(2, 2, "p1", true)}
		wantViolation(t, sim.WriteAtMostOnce(log), sim.PropWriteAtMostOnce, "applied again")
	})
	t.Run("replayed after a crash", func(t *testing.T) {
		log := sim.Log{write(1, 1, "p1", true), sim.ControllerDown{At: at(2)}, sim.ControllerUp{At: at(3)}, write(4, 1, "p1", true)}
		wantViolation(t, sim.WriteAtMostOnce(log), sim.PropWriteAtMostOnce, "1 controller or host crash")
	})
	t.Run("another binding or incident may write the same patch", func(t *testing.T) {
		other := write(2, 1, "p1", true)
		other.Attribution.Binding = "scale-api{api=Deployment/other}"
		later := write(3, 1, "p1", true)
		later.Attribution.Incident = "i2"
		wantOK(t, sim.WriteAtMostOnce(sim.Log{write(1, 1, "p1", true), other, later}))
	})
	t.Run("an unattributed write cannot be shown to apply once", func(t *testing.T) {
		log := sim.Log{sim.Write{At: at(1), Object: deploy, Patch: "p1", Committed: true}}
		wantViolation(t, sim.WriteAtMostOnce(log), sim.PropWriteAtMostOnce, "no binding")
	})
}

func boundedRun(entries ...sim.Entry) sim.Log {
	return append(sim.Log{sim.FaultsQuiesced{At: at(100)}}, append(entries, sim.RunEnded{At: at(400)})...)
}

// Property 3. Controller mutant: when a closure fails permanently, the
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
	t.Run("a run without bounded faults is rejected, not passed", func(t *testing.T) {
		wantViolation(t, sim.EveryIncidentEnds(sim.Log{open("i1", 10), sim.Outcome{At: at(20), Incident: "i1"}}, cfg), sim.PropEveryIncidentEnds, "not shown to be bounded")
		short := sim.Config{Settle: time.Hour}
		wantViolation(t, sim.EveryIncidentEnds(good, short), sim.PropEveryIncidentEnds, "settle time")
	})
}

// Property 4. Controller mutant: memory activation holds the evaluation lock
// while it compiles the new revision (the sim's compile delay), or closure
// waits on the agent inside the evaluation loop, so no detector runs for the
// duration.
func TestDetectionKeepsRunning(t *testing.T) {
	cfg := sim.Config{MaxDetectionGap: 30 * time.Second}
	evals := func(detector string, secs ...int) (out []sim.Entry) {
		for _, s := range secs {
			out = append(out, sim.Evaluated{At: at(s), Detector: detector})
		}
		return out
	}
	build := func(parts ...[]sim.Entry) sim.Log {
		log := sim.Log{sim.Activated{At: at(0), Revision: "r1", Detectors: []string{"pods"}}}
		for _, p := range parts {
			log = append(log, p...)
		}
		return append(log, sim.RunEnded{At: at(100)})
	}
	wantOK(t, sim.DetectionKeepsRunning(build(evals("pods", 20, 40, 60, 80, 99)), cfg))

	t.Run("gap in the middle", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(build(evals("pods", 10, 70, 90)), cfg), sim.PropDetectionRuns, `"pods" was not evaluated for 1m0s`)
	})
	t.Run("detector never evaluated", func(t *testing.T) {
		wantViolation(t, sim.DetectionKeepsRunning(build(), cfg), sim.PropDetectionRuns, "1m40s")
	})
	t.Run("silent after the last evaluation", func(t *testing.T) {
		log := sim.Log{sim.Activated{At: at(0), Revision: "r1", Detectors: []string{"pods"}}, sim.Evaluated{At: at(10), Detector: "pods"}, sim.RunEnded{At: at(100)}}
		wantViolation(t, sim.DetectionKeepsRunning(log, cfg), sim.PropDetectionRuns, "between")
	})
	t.Run("controller downtime is not counted", func(t *testing.T) {
		log := build(evals("pods", 10, 20), []sim.Entry{sim.ControllerDown{At: at(21)}, sim.ControllerUp{At: at(70)}}, evals("pods", 80, 99))
		wantOK(t, sim.DetectionKeepsRunning(log, cfg))
	})
	t.Run("a new revision's detector must start on time, a retired one need not", func(t *testing.T) {
		log := sim.Log{
			sim.Activated{At: at(0), Revision: "r1", Detectors: []string{"pods"}},
			sim.Evaluated{At: at(10), Detector: "pods"}, sim.Evaluated{At: at(20), Detector: "pods"},
			sim.Activated{At: at(25), Revision: "r2", Detectors: []string{"svc"}},
			sim.RunEnded{At: at(100)},
		}
		wantViolation(t, sim.DetectionKeepsRunning(log, cfg), sim.PropDetectionRuns, `"svc"`)
		log = append(log[:len(log)-1:len(log)-1], sim.Evaluated{At: at(40), Detector: "svc"}, sim.Evaluated{At: at(70), Detector: "svc"}, sim.Evaluated{At: at(99), Detector: "svc"}, sim.RunEnded{At: at(100)})
		wantOK(t, sim.DetectionKeepsRunning(log, cfg))
	})
}

// Property 5. Controller mutant: closing an incident resets the finding-state
// tracker (or clears the batcher), dropping findings that activated after the
// incident opened.
func TestNothingIsLost(t *testing.T) {
	base := []sim.Entry{
		sim.Opened{At: at(0), Incident: "i1", Findings: []sim.FindingRef{crash}},
		sim.Seen{At: at(5), Finding: lateF, Incident: "i1"},
		sim.Outcome{At: at(10), Incident: "i1", Result: sim.Closed},
	}
	with := func(rest ...sim.Entry) sim.Log { return append(append(sim.Log(nil), base...), rest...) }
	wantOK(t, sim.NothingIsLost(with(sim.Tracked{At: at(11), Keys: []string{lateF.Key}}, sim.Tracked{At: at(20), Keys: []string{lateF.Key}})))

	t.Run("dropped at closure", func(t *testing.T) {
		wantViolation(t, sim.NothingIsLost(with(sim.Tracked{At: at(11), Keys: nil})), sim.PropNothingLost, "no longer tracked")
	})
	t.Run("dropped later without being cleared", func(t *testing.T) {
		wantViolation(t, sim.NothingIsLost(with(sim.Tracked{At: at(11), Keys: []string{lateF.Key}}, sim.Tracked{At: at(30)})), sim.PropNothingLost, "no longer tracked")
	})
	t.Run("clearing releases the finding", func(t *testing.T) {
		wantOK(t, sim.NothingIsLost(with(sim.Tracked{At: at(11), Keys: []string{lateF.Key}}, sim.Cleared{At: at(20), Finding: lateF}, sim.Tracked{At: at(30)})))
	})
	t.Run("no snapshot after closure is unobserved, not fine", func(t *testing.T) {
		wantViolation(t, sim.NothingIsLost(with()), sim.PropNothingLost, "no tracked-set snapshot")
	})
	t.Run("a finding activating when no incident is open is not covered", func(t *testing.T) {
		log := sim.Log{sim.Seen{At: at(1), Finding: lateF}, sim.Tracked{At: at(2)}}
		wantOK(t, sim.NothingIsLost(log))
	})
}

// Property 6. Controller mutant: the controller loads the program registry once
// at startup and ignores later activations, so a program saved by reflection
// is not used until the controller restarts.
func TestSavedMemoryGetsUsed(t *testing.T) {
	act := sim.Activated{At: at(0), Revision: "r2", Detectors: []string{"pods"}, Programs: []sim.ProgramDecl{{ID: "fix-crash", Detectors: []string{"pods"}}}}
	opened := sim.Opened{At: at(10), Incident: "i1", Findings: []sim.FindingRef{crash}}
	good := sim.Log{act, opened, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByProgram, ID: "fix-crash", Revision: "r2"}, sim.Handler{At: at(20), Incident: "i1", Kind: sim.ByAgent, ID: "responder"}}
	wantOK(t, sim.SavedMemoryGetsUsed(good))

	t.Run("agent starts first", func(t *testing.T) {
		log := sim.Log{act, opened, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByAgent, ID: "responder"}}
		wantViolation(t, sim.SavedMemoryGetsUsed(log), sim.PropMemoryGetsUsed, "fix-crash")
	})
	t.Run("program of the wrong revision", func(t *testing.T) {
		log := sim.Log{act, opened, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByProgram, ID: "fix-crash", Revision: "r1"}, sim.Handler{At: at(12), Incident: "i1", Kind: sim.ByAgent}}
		wantViolation(t, sim.SavedMemoryGetsUsed(log), sim.PropMemoryGetsUsed, "r2")
	})
	t.Run("a program that declines may be skipped", func(t *testing.T) {
		log := sim.Log{act, opened, sim.ProgramDiagnosed{At: at(11), Incident: "i1", Program: "fix-crash", Revision: "r2", Result: sim.DiagError}, sim.Handler{At: at(12), Incident: "i1", Kind: sim.ByAgent}}
		wantOK(t, sim.SavedMemoryGetsUsed(log))
	})
	t.Run("an incident no program matches goes to the agent", func(t *testing.T) {
		other := sim.Opened{At: at(10), Incident: "i1", Findings: []sim.FindingRef{{Key: "svc\x00x", Detector: "svc"}}}
		wantOK(t, sim.SavedMemoryGetsUsed(sim.Log{act, other, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByAgent}}))
	})
	t.Run("a restart between activation and the incident hides the defect and is not a witness", func(t *testing.T) {
		log := sim.Log{act, sim.ControllerDown{At: at(5)}, sim.ControllerUp{At: at(6)}, opened, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByAgent}}
		wantOK(t, sim.SavedMemoryGetsUsed(log))
	})
	t.Run("only the next matching incident is judged, and a newer revision ends the window", func(t *testing.T) {
		second := sim.Opened{At: at(30), Incident: "i2", Findings: []sim.FindingRef{crash}}
		log := append(append(sim.Log(nil), good...), second, sim.Handler{At: at(31), Incident: "i2", Kind: sim.ByAgent})
		wantOK(t, sim.SavedMemoryGetsUsed(log))
		next := sim.Activated{At: at(5), Revision: "r3", Detectors: []string{"pods"}}
		wantOK(t, sim.SavedMemoryGetsUsed(sim.Log{act, next, opened, sim.Handler{At: at(11), Incident: "i1", Kind: sim.ByAgent}}))
	})
}

func TestCheckReportsEveryViolation(t *testing.T) {
	log := sim.Log{sim.Write{At: at(1), Object: deploy, Committed: true}}
	if got := sim.Check(log, sim.Config{MaxDetectionGap: time.Second}); len(got) < 3 {
		t.Fatalf("want violations of several properties, got %v", got)
	}
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
		// The stale read is exactly what property 1 rejects as proof of a fix.
		log = append(log, sim.Evaluated{At: w.Now(), Detector: "cfg", Observation: stale.ID}, sim.Fixed{At: w.Now(), Finding: sim.FindingRef{Key: "k", Detector: "cfg"}, Scope: []sdk.ObjectRef{ref}, Observation: stale.ID})
		wantViolation(t, sim.FixedMeansVerified(log), sim.PropFixedMeansVerified, "write 1")
	})
}

// The recorder implements the controller's existing lifecycle and firing sink
// interfaces, deduplicated by event id as the durable sinks are.
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
	for _, f := range []*contracts.FiringRecord{
		{EventId: "a", Event: "activated", DetectorId: "pods", Fingerprint: "late", IncidentId: "i1", DispatchRelation: "after_dispatch", RecordedAt: timestamppb.New(t0)},
		{EventId: "b", Event: "cleared", DetectorId: "pods", Fingerprint: "late", RecordedAt: timestamppb.New(t0)},
	} {
		if err := rec.Record(f); err != nil {
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
		case sim.Seen:
			kinds = append(kinds, "seen:"+x.Incident)
		case sim.Cleared:
			kinds = append(kinds, "cleared")
		}
	}
	want := "opened:i1:pods handler:agent outcome:i1:failed opened:i2:pods outcome:i2:closed seen:i1 cleared"
	if got := strings.Join(kinds, " "); got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}
