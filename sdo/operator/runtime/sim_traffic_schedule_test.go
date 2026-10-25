package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// The observation edge keeps the prober's process-local qualification rule.
// Controller restart schedules replace this edge along with the process; no
// qualification bit is copied from durable controller state.
type simTrafficSource struct {
	world  *simWorld
	window traffic.Window
}

func (s *simTrafficSource) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := s.world.cache.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	value := snapshot.(sdktest.Snapshot)
	value.Traffic = map[string]traffic.Window{"health": s.window}
	return value, nil
}
func (s *simTrafficSource) restart() {
	for i := range s.window.Scenarios {
		s.window.Scenarios[i].Qualified = false
		s.window.Scenarios[i].Samples = nil
	}
}
func (s *simTrafficSource) sample() {
	s.window.ObservedAt = s.world.clock.Now()
	for i := range s.window.Scenarios {
		observed := &s.window.Scenarios[i]
		ports, _, _ := unstructuredPorts(s.world.kube.objects[simServiceRef(simServiceName(i))].Object)
		sample := traffic.Sample{At: s.window.ObservedAt, Requests: 1, Outcome: traffic.OutcomeOK, Status: 200}
		if ports[0].(map[string]any)["targetPort"] == int64(9999) {
			sample.Outcome = traffic.OutcomeError
			sample.Status = 503
		} else {
			observed.Qualified = true
		}
		observed.Samples = append(observed.Samples, sample)
		if len(observed.Samples) > 3 {
			observed.Samples = observed.Samples[len(observed.Samples)-3:]
		}
	}
}

// TQ owns the pending/qualification contract tests. This adds only the seeded
// restart event to the durable incident simulation, with faults and repairs on
// the simulated API. Repeated restarts may never manufacture a healthy gap.
func runSimRestartOntoFailingTraffic(t *testing.T, seed uint64) {
	t.Helper()
	w := newSimWorld(t, seed)
	w.client.ReactionChain = w.client.ReactionChain[2:]
	w.responder.behavior = func() simBehavior { return simFixNone }
	w.broker.reject = func() bool { return false }
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","slo":{"window":3,"minSamples":3},"scenarios":[{"id":"search"},{"id":"login"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	source := &simTrafficSource{world: w, window: traffic.Window{Workload: workload}}
	for i, id := range []string{"search", "login"} {
		source.window.Scenarios = append(source.window.Scenarios, traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: id, Target: traffic.Target{Service: simServiceName(i), Port: 80}, SideEffect: traffic.SideEffectRead}})
	}
	spec := trafficDetector("traffic-health", "health").Spec()
	spec.Interval = time.Second
	w.detectors = []sdk.Detector{traffic.NewDetector(spec, "health")}
	w.configure = func(c *Controller) { c.provider = source }
	w.restart()
	fail := 12 + w.rng.IntN(8)
	restart := fail + 15 + w.rng.IntN(4)
	recover := restart + 15 + w.rng.IntN(5)
	events := []simScheduleEvent{{restart, simRestartFailingTraffic}, {restart + 5, simRestartFailingTraffic}}
	pendingSeen := 0
	before := 0
	for tick := 0; tick < recover+15; tick++ {
		w.clock.now = w.clock.now.Add(time.Second)
		if tick == fail {
			for i := 0; i < 2; i++ {
				w.setPort(simServiceName(i), 9999)
			}
		}
		if tick == recover {
			for i := 0; i < 2; i++ {
				w.setPort(simServiceName(i), 8080)
			}
		}
		for _, event := range events {
			if event.tick != tick {
				continue
			}
			if event.kind != simRestartFailingTraffic {
				t.Fatalf("unexpected event %q", event.kind)
			}
			if tick == restart {
				before = w.responder.calls
				if before != 1 || !w.controller.IncidentOpen() {
					t.Fatal("restart was not on a running failing incident")
				}
			}
			w.persist()
			source.restart()
			w.restart()
		}
		source.sample()
		w.step()
		w.drain()
		if tick >= restart && tick < recover {
			evaluation := w.controller.history[len(w.controller.history)-1]
			if evaluation.Status != DetectorEvaluationPending || evaluation.Reason == "" || w.controller.completeHealthyEvaluation() || len(w.closed) > 0 || w.responder.calls != before {
				t.Fatalf("restart onto failing traffic fabricated health or dispatch: evaluation=%+v closures=%d calls=%d", evaluation, len(w.closed), w.responder.calls)
			}
			pendingSeen++
		}
	}
	if pendingSeen == 0 || len(w.closed) != 1 || len(w.closed[0].FinalDetectorStates) == 0 || !w.idle() || len(w.violations) > 0 {
		t.Fatalf("scheduled restart/recovery did not settle: pending=%d closures=%+v violations=%v", pendingSeen, w.closed, w.violations)
	}
}
