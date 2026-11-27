package journeycheck

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

func soakSample(at time.Time, iteration uint64, outcome traffic.Outcome) traffic.Sample {
	return traffic.Sample{At: at, Iteration: iteration, Outcome: outcome}
}

// The controller's prober windows are bounded and slide; the installed soak
// keeps every sample since it began, once, and judges them all at the end
// with the same judgement as the in-session healthy run.
func TestInstalledSoakJudgesEverySampleSinceItBeganOnce(t *testing.T) {
	began := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	soak := NewInstalledSoak(began, 5*time.Minute)
	workload := traffic.Workload{Name: "probe-call", Purpose: traffic.PurposeHealthProbe}.WithDefaults()
	window := func(samples ...traffic.Sample) map[string]traffic.Window {
		return map[string]traffic.Window{"probe-call": {Workload: workload, Scenarios: []traffic.ScenarioObservations{
			{Scenario: traffic.Descriptor{ID: "sign-in"}, Qualified: true, Samples: samples},
		}}}
	}
	before := soakSample(began.Add(-time.Second), 1, traffic.OutcomeError)
	flake := soakSample(began.Add(time.Minute), 2, traffic.OutcomeError)
	soak.Observe(window(before, flake))
	// The failed sample slid out of the window; the soak still has it.
	soak.Observe(window(soakSample(began.Add(2*time.Minute), 3, traffic.OutcomeOK), soakSample(began.Add(3*time.Minute), 4, traffic.OutcomeOK)))
	soak.Observe(window(flake, soakSample(began.Add(2*time.Minute), 3, traffic.OutcomeOK)))
	if soak.Due(began.Add(4 * time.Minute)) {
		t.Fatal("due before its duration")
	}
	record := soak.Judge(began.Add(5 * time.Minute))
	red := record.Red()
	if len(red) != 1 || red[0].Subject != "probe-call/sign-in" || !strings.Contains(red[0].Detail, "1 of 3 samples failed") {
		t.Fatalf("want one red counting the in-soak samples once: %+v", record)
	}
	message := SoakFollowUp(record)
	if !strings.Contains(message, "probe-call/sign-in") || !strings.Contains(message, "one line") || len(message) > 2600 {
		t.Fatalf("follow-up message:\n%s", message)
	}
}

func TestGreenInstalledSoakSendsNoFollowUp(t *testing.T) {
	began := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	soak := NewInstalledSoak(began, time.Minute)
	soak.Observe(map[string]traffic.Window{"probe-call": {Workload: traffic.Workload{Purpose: traffic.PurposeHealthProbe}.WithDefaults(),
		Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "home"}, Qualified: true,
			Samples: []traffic.Sample{soakSample(began.Add(time.Second), 7, traffic.OutcomeOK)}}}}})
	record := soak.Judge(began.Add(time.Minute))
	if len(record.Red()) != 0 || SoakFollowUp(record) != "" || len(record.Results) != 1 {
		t.Fatalf("%+v", record)
	}
}

// Only health-probe workloads soak: link probes are not user journeys.
func TestInstalledSoakIgnoresOtherWorkloads(t *testing.T) {
	began := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	soak := NewInstalledSoak(began, time.Minute)
	soak.Observe(map[string]traffic.Window{"link": {Workload: traffic.Workload{Purpose: traffic.PurposeLinkProbe},
		Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "a-b"},
			Samples: []traffic.Sample{soakSample(began.Add(time.Second), 1, traffic.OutcomeError)}}}}})
	if record := soak.Judge(began.Add(time.Minute)); len(record.Results) != 0 {
		t.Fatalf("%+v", record)
	}
}

// Many red lines are bounded; the count of the rest is named.
func TestSoakFollowUpIsBounded(t *testing.T) {
	record := SoakRecord{Duration: traffic.Duration(5 * time.Minute)}
	for i := 0; i < 200; i++ {
		record.Results = append(record.Results, Result{ItemSoak, "probe/s" + strings.Repeat("x", i%7) + string(rune('a'+i%26)), Red, strings.Repeat("failed ", 20)})
	}
	message := SoakFollowUp(record)
	if len(message) > 2600 || !strings.Contains(message, "more red") {
		t.Fatalf("%d bytes:\n%s", len(message), message)
	}
}
