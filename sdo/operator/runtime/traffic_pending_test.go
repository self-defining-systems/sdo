package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func TestUnqualifiedTrafficIsPendingWithoutOpeningIncident(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"search"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	window := traffic.Window{Workload: workload, ObservedAt: now, Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "search", Target: traffic.Target{Service: "frontend", Port: 5000}}, Samples: []traffic.Sample{{At: now, Outcome: traffic.OutcomeError, Status: 500}}}}}
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": window}}}
	detector := trafficDetector("traffic-health", "health")
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if len(c.history) != 1 || c.history[0].Status != DetectorEvaluationPending {
		t.Fatalf("unqualified traffic read clear: %+v", c.history)
	}
	if c.completeHealthyEvaluation() {
		t.Fatal("pending traffic established the detector-clear baseline")
	}
	if !strings.Contains(c.history[0].Reason, "no synthetic scenario has succeeded") {
		t.Fatalf("pending reason missing: %+v", c.history)
	}
	if c.incidentOpen || len(c.tracker.Snapshot()) != 0 {
		t.Fatal("pending traffic opened an incident")
	}
	if _, healthy := c.latestEvaluations([]string{detector.Spec().ID}, time.Time{}, true); healthy {
		t.Fatal("pending traffic established clearance")
	}
	loop := NewResponderLoop(IncidentRequest{}, nil, provider, []sdk.Detector{detector}, PlanExecutor{}, 3)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return program.ReadFrom(snapshot) }
	for _, command := range []string{"detect", "verify", "status"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		if response.Healthy || len(response.Findings) != 0 || len(response.DetectorStates) != 1 || response.DetectorStates[0].Status != DetectorEvaluationPending || response.DetectorStates[0].Reason != c.history[0].Reason || !strings.Contains(response.Summary, "pending") {
			t.Fatalf("%s falsely healthy: %+v", command, response)
		}
	}
}

func TestScaledTrafficRetainsObservationConfirmation(t *testing.T) {
	detector := scaleDetectors([]sdk.Detector{trafficDetector("traffic-health", "health")}, .1)[0]
	if detectorConfirmed(detector, sdktest.Snapshot{}) {
		t.Fatal("scaled detector lost pending observation state")
	}
}

func TestPendingTrafficReasonsSurviveCompaction(t *testing.T) {
	history := []DetectorEvaluation{
		{DetectorID: "traffic-health", Status: DetectorEvaluationPending, Reason: "no synthetic scenario has succeeded since observation began: search, login"},
		{DetectorID: "traffic-health", Status: DetectorEvaluationPending, Reason: "no synthetic scenario has succeeded since observation began: search, login"},
		{DetectorID: "traffic-health", Status: DetectorEvaluationPending, Reason: "synthetic scenarios have not succeeded since observation began: login"},
	}
	if got := compactDetectorHistory(history); len(got) != 3 {
		t.Fatalf("lost qualification progress: %+v", got)
	}
}

func TestPendingObservationCannotConfirmSubmitGate(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	detector := unconfirmedDetector{inventoryHealth()}
	config := gateConfig()
	c, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	finding := stateFinding(detector.Spec().ID)
	c.tracker.SetPolicy(detector.Spec().ID, 1, 10, 0)
	c.tracker.Observe(now, detector.Spec().ID, []sdk.Finding{finding})
	c.observeGate(detector.Spec().ID, now, nil)
	c.observeGate(detector.Spec().ID, now.Add(time.Second), nil)
	c.probeGate(context.Background(), now.Add(2*time.Second), sdktest.Snapshot{}, []sdk.Detector{detector})
	if c.gateConfirmedKey(FindingStateKey(detector.Spec().ID, FindingFingerprint(finding)), now.Add(2*time.Second)) {
		t.Fatal("pending observation authorized submission")
	}
}

func TestClosureRevalidationRejectsPendingTraffic(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	detector := trafficDetector("traffic-health", "health")
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	closure := IncidentClosure{FinalDetectorStates: []DetectorEvaluation{{DetectorID: detector.Spec().ID, Status: DetectorEvaluationClear}}}
	if err := c.revalidateClosure(context.Background(), sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": {}}}, closure); err == nil || !strings.Contains(err.Error(), "unconfirmed") {
		t.Fatalf("closure must reject unconfirmed traffic explicitly: %v", err)
	}
}

// A process replacement loses steady qualification while retaining the incident.
// Recovery must be observable again, through the same verify transport and
// controller persistence used in production, without carrying old qualification.
func TestTrafficRestartMidIncidentCanVerifyRecovery(t *testing.T) {
	ctx := context.Background()
	frontend := &fakeFrontend{}
	firstAPI := runProber(t, frontend)
	observer := NewTrafficObserver(firstAPI, []string{"health"}, nil, 0)
	detector := trafficDetector("traffic-health", "health")
	waitWindow := func(ready func(traffic.Window) bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			observer.Poll(ctx)
			if ready(observer.Windows()["health"]) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("probe window did not become ready: %+v", observer.Windows())
	}
	waitWindow(func(w traffic.Window) bool {
		return detectorConfirmed(detector, sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": w}})
	})
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "hotel-reservation"
	config.VerificationTimeout = time.Minute
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	provider := TrafficSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: config.Namespace}}, Observer: observer}
	c, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	frontend.broken.Store(true)
	waitWindow(func(w traffic.Window) bool {
		verdicts := traffic.Judge(w)
		if len(verdicts) != 2 {
			return false
		}
		for _, v := range verdicts {
			if !v.Evaluated || v.Healthy {
				return false
			}
		}
		return true
	})
	step := func(controller *Controller, i int) {
		t.Helper()
		if err := controller.StepEvents(ctx, start.Add(time.Duration(i)*time.Second), []sdk.WatchKind{traffic.Watch}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= 10; i++ {
		step(c, i)
	}
	executePendingEffect(t, c)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, c)
	step(c, 11)
	if !c.IncidentOpen() {
		t.Fatal("failing traffic closed before restart")
	}
	state := exportState(t, c)
	// A new process is constructed while the same application is still broken.
	restartedAPI := runProber(t, frontend)
	observer = NewTrafficObserver(restartedAPI, []string{"health"}, nil, 0)
	waitWindow(func(w traffic.Window) bool {
		if len(w.Scenarios) != 2 {
			return false
		}
		for _, s := range w.Scenarios {
			if s.Qualified || len(s.Samples) < 3 {
				return false
			}
		}
		return true
	})
	provider.Observer = observer
	restored, err := NewController(config, []sdk.Detector{trafficDetector("traffic-health", "health")}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start.Add(12*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	step(restored, 12)
	if got := restored.history[len(restored.history)-1]; got.Status != DetectorEvaluationPending || !strings.Contains(got.Reason, "no synthetic scenario has succeeded") {
		t.Fatalf("restart fabricated health: %+v", got)
	}
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(ctx context.Context) ToolResponse {
		return restored.cachedResponderVerification(ctx, request.IncidentID, 0)
	}
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if response := exchangeSelfCheck(t, &server, `{"command":"verify"}`); response.Healthy {
		t.Fatal("verify accepted failed unqualified traffic")
	}
	frontend.broken.Store(false)
	burst, err := restartedAPI.Burst(ctx, prober.BurstRequest{Workload: "verify"})
	if err != nil || !burst.Healthy || len(burst.Verdicts) != 2 {
		t.Fatalf("fresh recovered verify burst: %+v %v", burst, err)
	}
	for _, verdict := range burst.Verdicts {
		if !verdict.Evaluated || !verdict.Healthy {
			t.Fatalf("burst did not judge recovered scenario: %+v", verdict)
		}
	}
	waitWindow(func(w traffic.Window) bool {
		if !detectorConfirmed(detector, sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": w}}) {
			return false
		}
		for _, v := range traffic.Judge(w) {
			if !v.Evaluated || !v.Healthy {
				return false
			}
		}
		return true
	})
	step(restored, 13)
	if response := exchangeSelfCheck(t, &server, `{"command":"verify"}`); response.Healthy {
		t.Fatal("verify skipped ordinary clearing persistence")
	}
	if _, healthy := restored.finalVerificationStates(); healthy {
		t.Fatal("first recovered sample cleared active traffic findings")
	}
	var closure IncidentClosure
	restored.OnIncidentClosed = func(closed IncidentClosure) { closure = closed }
	step(restored, 14)
	verified := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	if !verified.Healthy || restored.IncidentOpen() || closure.Request.IncidentID != request.IncidentID || len(closure.FinalDetectorStates) != 1 || closure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
		t.Fatalf("restart could not verify/close recovered incident: verified=%+v closure=%+v", verified, closure)
	}
}

func TestEvaluateOnceUnqualifiedTrafficReportsPendingReason(t *testing.T) {
	frontend := &fakeFrontend{}
	frontend.broken.Store(true)
	api := runProber(t, frontend)
	address, err := api.BaseURL(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, accept := range []bool{false, true} {
		var output bytes.Buffer
		args := []string{"--evaluate-once", "--namespace", "hotel-reservation", "--prober-url", address}
		if accept {
			args = append(args, "--accept-seed")
		}
		err := RunWithOptions(context.Background(), []sdk.Detector{trafficDetector("traffic-health", "health")}, RuntimeOptions{Args: args, Stdout: &output, Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "hotel-reservation"}}})
		if (err != nil) != accept {
			t.Fatalf("accept=%v: %v", accept, err)
		}
		var response evaluationResult
		if err := json.Unmarshal(output.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Status != evaluationStatus(DetectorEvaluationPending) || len(response.Findings) != 0 || len(response.DetectorStates) != 1 || response.DetectorStates[0].Status != DetectorEvaluationPending || !strings.Contains(response.DetectorStates[0].Reason, "no synthetic scenario has succeeded") {
			t.Fatalf("unproven traffic read-only verdict: %s", output.String())
		}
	}
}

func TestTrafficSelfChecksRetainPartialQualificationAndRealFaults(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	workload, err := traffic.ParseWorkloadJSON([]byte(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"health","purpose":"health-probe","scenarios":[{"id":"search"},{"id":"login"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	scenario := func(id string, qualified, failed bool) traffic.ScenarioObservations {
		observed := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: id, Target: traffic.Target{Service: "frontend", Port: 5000}}, Qualified: qualified}
		for i := 0; i < 3; i++ {
			sample := traffic.Sample{At: now.Add(time.Duration(i-2) * time.Second), Outcome: traffic.OutcomeOK, Status: 200}
			if failed {
				sample.Outcome, sample.Status = traffic.OutcomeError, 500
			}
			observed.Samples = append(observed.Samples, sample)
		}
		return observed
	}
	for _, test := range []struct {
		name         string
		observations []traffic.ScenarioObservations
		status       DetectorEvaluationStatus
		findings     int
		unproven     string
	}{
		{"qualified-healthy", []traffic.ScenarioObservations{scenario("search", true, false), scenario("login", true, false)}, DetectorEvaluationClear, 0, ""},
		{"qualified-fault", []traffic.ScenarioObservations{scenario("search", true, true), scenario("login", true, false)}, DetectorEvaluationFiring, 1, ""},
		{"partial", []traffic.ScenarioObservations{scenario("search", true, false), scenario("login", false, true)}, DetectorEvaluationPending, 0, "login"},
		{"partial-fault", []traffic.ScenarioObservations{scenario("search", true, true), scenario("login", false, true)}, DetectorEvaluationFiring, 1, "login"},
	} {
		for _, command := range []string{"detect", "status", "verify"} {
			t.Run(test.name+"/"+command, func(t *testing.T) {
				provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"health": {Workload: workload, ObservedAt: now, Scenarios: test.observations}}}}
				loop := NewResponderLoop(IncidentRequest{}, nil, provider, []sdk.Detector{trafficDetector("traffic-health", "health")}, PlanExecutor{}, 3)
				loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView { return program.ReadFrom(snapshot) }
				response := loop.Handle(context.Background(), ToolRequest{Command: command})
				if response.Error != "" || response.Healthy != (test.status == DetectorEvaluationClear) || len(response.Findings) != test.findings || len(response.DetectorStates) != 1 || response.DetectorStates[0].Status != test.status {
					t.Fatalf("self-check changed traffic semantics: %+v", response)
				}
				reason := response.DetectorStates[0].Reason
				if test.unproven == "" && reason != "" || test.unproven != "" && !strings.Contains(reason, test.unproven) {
					t.Fatalf("missing unproven scenario explanation: %+v", response.DetectorStates)
				}
			})
		}
	}
}
