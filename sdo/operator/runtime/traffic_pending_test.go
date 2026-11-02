package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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

func TestClosureRevalidationReportsPendingTrafficAsIncomplete(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	detector := trafficDetector("traffic-health", "health")
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	closure := IncidentClosure{FinalDetectorStates: []DetectorEvaluation{{DetectorID: detector.Spec().ID, Status: DetectorEvaluationClear}}}
	recheck := c.revalidateClosure(context.Background(), sdktest.Snapshot{Traffic: map[string]traffic.Window{"health": {}}}, closure)
	if recheck.Verdict != closureRecheckIncomplete || !strings.Contains(recheck.Reason, "unconfirmed") || recheck.Bound <= 0 {
		t.Fatalf("pending traffic is incomplete, never confirmed or contradicted: %+v", recheck)
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

