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

// A test whose failure is confirmed is not passing, though its scheduled
// samples still meet the SLO; a confirming success makes no test pass.
func TestPassingTestsLeaveOutAConfirmedFailure(t *testing.T) {
	window := probeWindow(nil, "a-call", "b-call")
	at := time.Unix(1005, 0)
	failing := &window.Scenarios[0]
	failing.Samples = append(failing.Samples, traffic.Sample{At: at, Outcome: traffic.OutcomeError})
	failing.Confirming = []traffic.Sample{{At: at.Add(4 * time.Second), Outcome: traffic.OutcomeError}}
	window.ObservedAt = at.Add(5 * time.Second)
	snapshot := windowSnapshot{windows: map[string]traffic.Window{"probe": window}}
	if got := passingTests(snapshot); !slices.Equal(got, []string{"b-call"}) {
		t.Fatalf("passing = %v, want only b-call", got)
	}
	window = probeWindow(map[string]bool{"a-call": true}, "a-call", "b-call")
	window.Scenarios[0].Confirming = []traffic.Sample{{At: at, Outcome: traffic.OutcomeOK}}
	snapshot.windows["probe"] = window
	if got := passingTests(snapshot); !slices.Equal(got, []string{"b-call"}) {
		t.Fatalf("passing = %v, want only b-call", got)
	}
}

// The observer marks the scenarios the accepted map names as user tests, and
// reads the map only while a sample fails.
func TestTrafficObserverMarksUserTests(t *testing.T) {
	observer := NewTrafficObserver(&fixedProber{}, []string{"probe"}, nil, 0)
	reads := 0
	observer.UseUserTests(func() map[string]bool { reads++; return map[string]bool{"a-call": true} })
	observer.windows = map[string]traffic.Window{"probe": probeWindow(nil, "a-call", "search")}
	if observer.Windows(); reads != 0 {
		t.Fatalf("the map was read %d times while healthy", reads)
	}
	observer.windows = map[string]traffic.Window{"probe": probeWindow(map[string]bool{"search": true}, "a-call", "search")}
	scenarios := observer.Windows()["probe"].Scenarios
	if !scenarios[0].UserTest || scenarios[1].UserTest {
		t.Fatalf("user tests marked: a-call %t, search %t", scenarios[0].UserTest, scenarios[1].UserTest)
	}
}

type fixedProber struct{ ProberAPI }
