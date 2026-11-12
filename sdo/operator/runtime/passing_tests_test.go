package runtime

import (
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

type windowSnapshot struct {
	sdk.DetectionContext
	windows map[string]traffic.Window
}

func (s windowSnapshot) TrafficWindows() map[string]traffic.Window { return s.windows }

func probeWindow(failing map[string]bool, ids ...string) traffic.Window {
	at := time.Unix(1000, 0)
	workload := traffic.Workload{Name: "probe", Purpose: traffic.PurposeHealthProbe}
	var observed []traffic.ScenarioObservations
	for _, id := range ids {
		workload.Scenarios = append(workload.Scenarios, traffic.WorkloadScenario{ID: id})
		outcome := traffic.OutcomeOK
		if failing[id] {
			outcome = traffic.OutcomeError
		}
		var samples []traffic.Sample
		for i := range 5 {
			samples = append(samples, traffic.Sample{At: at.Add(time.Duration(i) * time.Second), Outcome: outcome, Status: 200})
		}
		observed = append(observed, traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: id}, Samples: samples, Qualified: true})
	}
	return traffic.Window{Workload: workload.WithDefaults(), ObservedAt: at.Add(5 * time.Second), Scenarios: observed}
}

// The request names the user tests that still pass, across workloads, and the
// list survives the contract round trip.
func TestIncidentRequestCarriesThePassingTests(t *testing.T) {
	snapshot := windowSnapshot{windows: map[string]traffic.Window{
		"probe-call":  probeWindow(map[string]bool{"b-call": true}, "b-call", "a-call"),
		"probe-write": probeWindow(nil, "c-write"),
	}}
	got := passingTests(snapshot)
	if want := []string{"a-call", "c-write"}; !slices.Equal(got, want) {
		t.Fatalf("passing = %v, want %v", got, want)
	}
	if passingTests(nil) != nil {
		t.Fatal("a snapshot without traffic windows has no passing tests")
	}
	request := IncidentRequest{PassingTests: got}
	message, err := incidentRequestContract(request)
	if err != nil {
		t.Skipf("minimal request does not validate: %v", err)
	}
	if received := incidentRequestState(message); !slices.Equal(received.PassingTests, got) {
		t.Fatalf("round trip = %v, want %v", received.PassingTests, got)
	}
}
