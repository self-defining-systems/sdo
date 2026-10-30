package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func recoveryTrafficSnapshot(t *testing.T, now time.Time, latency time.Duration) sdktest.Snapshot {
	t.Helper()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"read","slo":{"latencyPercentile":50,"maxLatency":"250ms"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	samples := make([]traffic.Sample, 5)
	for i := range samples {
		samples[i] = traffic.Sample{At: now.Add(time.Duration(i-4) * time.Second), Outcome: traffic.OutcomeOK, Status: 200, Latency: latency}
	}
	return sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: workload, ObservedAt: now, Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "read", Target: traffic.Target{Service: "api", Port: 80}}, Qualified: true, Samples: samples}}}}}
}

// Real program execution produces the repair result; controller observation,
// proof publication and broker recheck consume the same traffic detector.
func repairedTrafficController(t *testing.T) (*Controller, *changingProvider, time.Time) {
	t.Helper()
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	result, err := responsePipelineResult(state, now.Add(-time.Second), now)
	if err != nil {
		t.Fatal(err)
	}
	provider := &changingProvider{snapshot: recoveryTrafficSnapshot(t, now, 100*time.Millisecond)}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.VerificationTimeout = time.Minute
	c, err := NewController(cfg, []sdk.Detector{trafficDetector("traffic-health", "health")}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.currentIncidentRequest = cloneIncidentRequest(&state.Request)
	c.currentIncidentRequest.PriorStages = cloneStageReports(state.PriorStages)
	c.incidentOpen = true
	c.incidentDispatchedAt = now.Add(-time.Second)
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.handleDispatchCompletion(dispatchCompletion{incidentID: state.Request.IncidentID, result: result, priorStages: state.PriorStages}, now)
	c.now = func() time.Time { return now }
	return c, provider, now
}

func TestRecoveryProofRequiresFreshTrafficAtPublication(t *testing.T) {
	c, p, start := repairedTrafficController(t)
	step := func(seconds int, latency time.Duration) {
		t.Helper()
		now := start.Add(time.Duration(seconds) * time.Second)
		p.snapshot = recoveryTrafficSnapshot(t, now, latency)
		if err := c.Step(context.Background(), now, nil); err != nil {
			t.Fatal(err)
		}
	}
	step(0, 100*time.Millisecond)
	step(10, 100*time.Millisecond)
	// No scheduled sample is due at 15s. A cached green must not publish a
	// proof over a fresh red window, even on the model-free completion path.
	step(15, 330*time.Millisecond)
	if _, ok := c.PendingIncidentClosure(); ok {
		t.Fatal("published recovery from an older green traffic epoch")
	}
	if !c.IncidentOpen() {
		t.Fatal("traffic settling lost incident ownership")
	}
	for _, seconds := range []int{16, 20, 30} {
		step(seconds, 100*time.Millisecond)
		if _, ok := c.PendingIncidentClosure(); ok {
			t.Fatalf("published before renewed stability at %ds", seconds)
		}
	}
	step(31, 100*time.Millisecond)
	closure, ok := c.PendingIncidentClosure()
	if !ok || len(closure.FinalDetectorStates) != 1 {
		t.Fatalf("fresh traffic did not close after stability: %+v", closure)
	}
	if got := c.revalidateClosure(context.Background(), p.snapshot, closure); got.Verdict != closureRecheckConfirmed {
		t.Fatalf("proof/recheck disagree: %+v", got)
	}
}

func TestClosureActiveTrafficWaitsThenRecoversOrHandsOff(t *testing.T) {
	for _, clears := range []bool{true, false} {
		t.Run(map[bool]string{true: "settles", false: "residual"}[clears], func(t *testing.T) {
			c, p, start := repairedTrafficController(t)
			broker := &recordingClosureBroker{}
			if err := c.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			now := start
			c.now = func() time.Time { return now }
			for _, seconds := range []int{0, 10, 15} {
				now = start.Add(time.Duration(seconds) * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				if err := c.Step(context.Background(), now, nil); err != nil {
					t.Fatal(err)
				}
			}
			if c.IncidentOpen() {
				t.Fatal("fixture did not produce the real recovery proof")
			}
			now = start.Add(16 * time.Second)
			p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
			executeRecheck(t, c)
			assertStillPending(t, c, broker)
			if clears {
				now = start.Add(17 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, c)
				assertStillPending(t, c, broker)
				now = start.Add(32 * time.Second)
				p.snapshot = recoveryTrafficSnapshot(t, now, 100*time.Millisecond)
				executeRecheck(t, c)
				awaitBrokerResult(t, c.closureResults)
				c.processBrokerCompletions()
				if broker.processCalls() != 1 || c.closureFailure != nil {
					t.Fatal("settled traffic did not commit")
				}
			} else {
				now = start.Add(time.Minute)
				p.snapshot = recoveryTrafficSnapshot(t, now, 330*time.Millisecond)
				executeRecheck(t, c)
				if c.closureFailure != nil || !c.IncidentOpen() {
					t.Fatalf("red traffic became permanent failure instead of residual: %+v", c.closureFailure)
				}
				if err := c.Step(context.Background(), now, nil); err != nil {
					t.Fatal(err)
				}
				effect, ok := c.PendingDispatchEffect()
				if !ok {
					t.Fatal("unrecovered automatic repair did not hand residual to responder")
				}
				if len(effect.Request.Findings) == 0 || len(effect.Request.PriorStages) == 0 || !effect.Request.ResponseDeadline.Equal(c.currentIncidentRequest.ResponseDeadline) {
					t.Fatal("handoff lost residuals, receipts or original budget")
				}
			}
		})
	}
}
