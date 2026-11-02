package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// OnEvaluation is a partial sample of the selected detectors. Resource-only
// iterations can be empty while traffic's last result remains 5/5 HTTP500.
// Those partial samples must never clear traffic state or verify closure.
func TestTrafficPartialIterationDoesNotClearFailingTrafficOrClose(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"search"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	samples := []traffic.Sample{}
	for i := 0; i < 5; i++ {
		samples = append(samples, traffic.Sample{At: start.Add(time.Duration(i) * time.Second), Status: 500, Outcome: traffic.OutcomeError, Requests: 1})
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: workload, ObservedAt: start.Add(5 * time.Second), Scenarios: []traffic.ScenarioObservations{{Qualified: true, Scenario: traffic.Descriptor{ID: "search", Target: traffic.Target{Service: "frontend", Port: 5000}, SideEffect: traffic.SideEffectRead, Steps: []string{"search"}}, Samples: samples}}}}}
	config := testControllerConfig()
	config.Namespace = "app"
	config.VerificationTimeout = time.Minute
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	c, err := NewController(config, []sdk.Detector{inventoryHealth(), trafficDetector("traffic-health", "health")}, staticProvider{snapshot: snapshot}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	empty := 0
	c.OnEvaluation = func(findings []sdk.Finding) {
		if len(findings) == 0 {
			empty++
		}
	}
	closed := 0
	c.OnIncidentClosed = func(IncidentClosure) { closed++ }
	step := func(i int) {
		t.Helper()
		var events []sdk.WatchKind
		if i%2 == 0 {
			events = []sdk.WatchKind{traffic.Watch}
		}
		if err := c.StepEvents(context.Background(), start.Add(time.Duration(i)*time.Second), events); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= 10; i++ {
		step(i)
	}
	executePendingEffect(t, c)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	for i := 11; i < 20; i++ {
		step(i)
		if check := c.responderVerification(context.Background(), request.IncidentID); check.Healthy {
			t.Fatalf("partial empty sample verified failing traffic at iteration%d", i)
		}
	}
	if empty == 0 || closed != 0 || !c.tracker.HasActiveDetector("traffic-health") {
		t.Fatalf("partial iterations=%d closures=%d active=%v", empty, closed, c.tracker.HasActiveDetector("traffic-health"))
	}
	for _, evaluation := range c.history {
		if evaluation.DetectorID == "traffic-health" && evaluation.Status != DetectorEvaluationFiring {
			t.Fatalf("5/5 failures became clear: %+v", evaluation)
		}
	}
	t.Logf("%d resource-only empty samples; traffic remained firing and closure stayed blocked", empty)
}
