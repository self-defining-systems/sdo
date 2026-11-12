package traffic

import (
	"net/http"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// A failing scenario's finding carries the symptom a person sees: when the
// window's first failing sample happened, the last failure's status and what
// the server said, so a reader need not parse the evidence text.
func TestScenarioFindingCarriesTheSymptom(t *testing.T) {
	base := time.Unix(1000, 0).UTC()
	samples := []Sample{
		{At: base, Outcome: OutcomeOK, Status: 200},
		{At: base.Add(2 * time.Second), Outcome: OutcomeError, Status: http.StatusTooManyRequests, Error: "HTTP 429 want 200", Observed: `text "Retry later"`},
		{At: base.Add(4 * time.Second), Outcome: OutcomeError, Status: http.StatusTooManyRequests, Error: "HTTP 429 want 200", Observed: `text "Retry later"`},
		{At: base.Add(6 * time.Second), Outcome: OutcomeError, Status: http.StatusTooManyRequests, Error: "HTTP 429 want 200", Observed: `text "Retry later"`},
	}
	workload := Workload{Name: "probe-call", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{{ID: "commit-call"}}}.WithDefaults()
	verdict := Evaluate("commit-call", workload.ScenarioSLO("commit-call"), samples, base.Add(6*time.Second))
	if verdict.Healthy {
		t.Fatal("setup: the scenario should violate its SLO")
	}
	finding := ScenarioFinding(sdk.DetectorSpec{ID: "traffic-probe-call"}, "app", workload, Descriptor{ID: "commit-call", Target: Target{Service: "web", Port: 80}}, verdict)
	want := map[string]any{
		"first_failure_at": base.Add(2 * time.Second).Format(time.RFC3339Nano),
		"failure_status":   "429",
		"failure_observed": `text "Retry later"`,
	}
	for key, value := range want {
		if finding.Metadata[key] != value {
			t.Errorf("metadata[%q] = %v, want %v", key, finding.Metadata[key], value)
		}
	}
}

// With no server answer the observed text falls back to the probe's own error.
func TestScenarioFindingObservedFallsBackToTheProbeError(t *testing.T) {
	base := time.Unix(1000, 0).UTC()
	var samples []Sample
	for i := range 4 {
		samples = append(samples, Sample{At: base.Add(time.Duration(i) * time.Second), Outcome: OutcomeError, Error: "build: bad template"})
	}
	workload := Workload{Name: "probe-call", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{{ID: "x"}}}.WithDefaults()
	verdict := Evaluate("x", workload.ScenarioSLO("x"), samples, base.Add(3*time.Second))
	finding := ScenarioFinding(sdk.DetectorSpec{ID: "traffic-probe-call"}, "app", workload, Descriptor{ID: "x", Target: Target{Service: "web", Port: 80}}, verdict)
	if finding.Metadata["failure_observed"] != "build: bad template" || finding.Metadata["failure_status"] != "transport" {
		t.Fatalf("metadata = %v", finding.Metadata)
	}
}
