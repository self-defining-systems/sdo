package journeycheck

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/traffic"
)

func listingWindow(marked ...int) map[string]traffic.Window {
	o := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: "read-existing"}, Qualified: true}
	for _, n := range marked {
		o.Samples = append(o.Samples, traffic.Sample{Outcome: traffic.OutcomeOK, ExistingRecords: traffic.ExistingRecordsExercised, ExistingMarked: n})
	}
	return map[string]traffic.Window{"probe-read": {Scenarios: []traffic.ScenarioObservations{o}}}
}

// Probe data left behind (a delete the application only scheduled) shows up
// as more and more probe-marked records in a list users read, though every
// sample passes: the healthy run is red, because the pile crowds users' own
// records off what they see.
func TestProbeRecordsPilingUpInAUsersListAreRed(t *testing.T) {
	got := Soaked(listingWindow(0, 1, 3, 5, 8))
	if len(got) != 1 || got[0].Status != Red || !strings.Contains(got[0].Detail, "probe-marked") {
		t.Fatalf("%+v", got)
	}
}

// A fixed fixture and the objects of iterations in flight are bounded.
func TestBoundedProbeRecordsInAUsersListAreGreen(t *testing.T) {
	if got := Soaked(listingWindow(1, 2, 1, 3, 1, 2)); len(got) != 1 || got[0].Status != Green {
		t.Fatalf("%+v", got)
	}
}

func TestSoakKeepsPassingScenariosWhenAnotherIsRedOrUnselected(t *testing.T) {
	w := traffic.Workload{
		Name: "probe", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 0.3,
		Scenarios: []traffic.WorkloadScenario{{ID: "ready"}, {ID: "broken"}, {ID: "unchecked"}},
	}.WithDefaults()
	report := Report{Results: []Result{
		{Item: ItemPasses, Subject: "ready", Status: Green},
		{Item: ItemPasses, Subject: "broken", Status: Green},
		{Item: ItemAltered, Subject: "broken", Status: Red},
	}}
	got := SoakWorkloads([]traffic.Workload{w}, report)
	if len(got) != 1 || len(got[0].Scenarios) != 1 || got[0].Scenarios[0].ID != "ready" {
		t.Fatalf("soak scheduled %v, want only the passing scenario", got)
	}
	if got[0].ScenarioArrival("ready") != w.ScenarioArrival("ready") {
		t.Fatalf("scoped soak changed cadence from %s to %s", w.ScenarioArrival("ready"), got[0].ScenarioArrival("ready"))
	}
	if len(w.Scenarios) != 3 {
		t.Fatal("soak changed the original schedule")
	}
}
