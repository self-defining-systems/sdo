package traffic

import (
	"fmt"
	"testing"
	"time"
)

// mixedWorkload is a health probe with reads read-only scenarios and writes
// scenarios that write, in that order, at the given rate.
func mixedWorkload(rate float64, reads, writes int) Workload {
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-mixed", Purpose: PurposeHealthProbe, RatePerSecond: rate,
	}
	for index := 0; index < reads; index++ {
		workload.Scenarios = append(workload.Scenarios, WorkloadScenario{ID: fmt.Sprintf("read-%02d", index), Weight: 1})
	}
	for index := 0; index < writes; index++ {
		workload.Scenarios = append(workload.Scenarios, WorkloadScenario{ID: fmt.Sprintf("write-%02d", index), Weight: 1, Writes: true})
	}
	return workload.WithDefaults()
}

func maxGap(times []time.Duration) time.Duration {
	var gap time.Duration
	for i := 1; i < len(times); i++ {
		gap = max(gap, times[i]-times[i-1])
	}
	return gap
}

// Reads keep the 10 s floor; writes are sampled at the longer write floor and
// never more often than it needs.
func TestWriteScenariosGetTheLongerFloor(t *testing.T) {
	for _, tc := range []struct {
		name            string
		reads, writes   int
		interval        time.Duration
		wantWriteGap    time.Duration
		wantRaisedRate  float64
		writeSamplesIn5 int
	}{
		{"default 20s", 3, 5, 0, 20 * time.Second, 6.0 / 10, 5 * 15},
		{"explicit 30s", 3, 5, 30 * time.Second, 30 * time.Second, 5.0 / 10, 5 * 10},
		{"writes only", 0, 5, 30 * time.Second, 30 * time.Second, 2.0 / 10, 6 * 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workload := mixedWorkload(0.01, tc.reads, tc.writes)
			workload.WriteSampleInterval = Duration(tc.interval)
			plan := workload.SamplingPlan()
			if !plan.Raised {
				t.Fatal("a tiny rate must be raised to meet the floors")
			}
			times := scenarioTimes(plan, 5*time.Minute)
			for _, scenario := range workload.Scenarios {
				want := MinSampleInterval
				if scenario.Writes {
					want = tc.wantWriteGap
				}
				if gap := maxGap(times[scenario.ID]); gap > want+time.Millisecond {
					t.Fatalf("%s sampled %s apart, floor is %s", scenario.ID, gap, want)
				}
				if arrival := workload.ScenarioArrival(scenario.ID); arrival > want+time.Millisecond {
					t.Fatalf("%s: ScenarioArrival %s exceeds %s", scenario.ID, arrival, want)
				}
			}
			writeSamples := 0
			for _, scenario := range workload.Scenarios {
				if scenario.Writes {
					writeSamples += len(times[scenario.ID])
				}
			}
			if writeSamples > tc.writeSamplesIn5+3*tc.writes {
				t.Fatalf("%d write samples in 5 minutes, want about %d: writes are sampled more often than their floor", writeSamples, tc.writeSamplesIn5)
			}
			if plan.Rate > tc.wantRaisedRate+1e-9 {
				t.Fatalf("plan rate %.3f, want at most %.3f", plan.Rate, tc.wantRaisedRate)
			}
		})
	}
}

// A write interval at or below the read floor is the old single floor.
func TestWriteIntervalAtTheReadFloorChangesNothing(t *testing.T) {
	workload := mixedWorkload(0.01, 3, 5)
	workload.WriteSampleInterval = Duration(2 * time.Second)
	plan := workload.SamplingPlan()
	if len(plan.Schedule) != 8 || plan.Rate != 0.8 {
		t.Fatalf("plan %v at %.2f/s, want every scenario once per 10s", plan.Schedule, plan.Rate)
	}
	if got := workload.WriteInterval(); got != MinSampleInterval {
		t.Fatalf("WriteInterval %s, want the read floor", got)
	}
}

// Without writes the plan is the read floor's plan, whatever the write interval.
func TestReadOnlyProbeIgnoresTheWriteInterval(t *testing.T) {
	workload := mixedWorkload(0.01, 4, 0)
	workload.WriteSampleInterval = Duration(2 * time.Minute)
	plan := workload.SamplingPlan()
	if len(plan.Schedule) != 4 || plan.Rate != 0.4 {
		t.Fatalf("plan %v at %.2f/s, want every read once per 10s", plan.Schedule, plan.Rate)
	}
}

// Writes cut the probe's request rate against the old single floor.
func TestWriteFloorLowersTheProbeLoad(t *testing.T) {
	workload := mixedWorkload(0.01, 3, 5)
	requests := map[string]int{}
	for _, scenario := range workload.Scenarios {
		requests[scenario.ID] = 2
	}
	longer := MeasureProbeLoad([]Workload{workload}, requests)
	workload.WriteSampleInterval = Duration(MinSampleInterval)
	single := MeasureProbeLoad([]Workload{workload}, requests)
	if single.RequestsPerSecond != 1.6 {
		t.Fatalf("single floor sends %.2f requests/s, want 1.6", single.RequestsPerSecond)
	}
	if want := 1.2; longer.RequestsPerSecond > want+1e-9 {
		t.Fatalf("write floor sends %.3f requests/s, want at most %.3f", longer.RequestsPerSecond, want)
	}
}

// A scenario sampled every write interval must still be able to hold MinSamples samples
// inside its window, so a write-only fault can be confirmed.
func TestSparseWriteScenarioWindowHoldsMinSamples(t *testing.T) {
	workload := mixedWorkload(0.01, 1, 1)
	slo := workload.ScenarioSLO("write-00")
	if need := time.Duration(slo.MinSamples) * workload.ScenarioArrival("write-00"); slo.MaxAge.Duration() < need {
		t.Fatalf("write scenario maxAge %s cannot hold %d samples %s apart", slo.MaxAge.Duration(), slo.MinSamples, workload.ScenarioArrival("write-00"))
	}
	if got := workload.ScenarioSLO("read-00").MaxAge.Duration(); got != DefaultMaxAge {
		t.Fatalf("read scenario maxAge %s, want the default %s", got, DefaultMaxAge)
	}
}

// The engine, not the author, says which scenarios write: it derives Writes
// from the catalog's side effects.
func TestEngineMarksWriteScenariosFromTheCatalog(t *testing.T) {
	read := laterScenario()
	write := Scenario{ID: "write-item", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectIdempotentWrite,
		Marker: "sdo-synthetic", Steps: []Step{{Name: "put", Endpoint: POST("/items", Params{"name": Const("sdo-synthetic-item")})}}}
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-mixed", Purpose: PurposeHealthProbe,
		// An author claiming the opposite is overwritten.
		Scenarios: []WorkloadScenario{{ID: read.ID, Writes: true}, {ID: write.ID}},
	}
	engine, err := NewEngine(workload, Catalog{read, write}, &itemStore{}, func(Target) string { return "http://web:80" }, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range engine.Workload().Scenarios {
		if want := selected.ID == write.ID; selected.Writes != want {
			t.Fatalf("scenario %s Writes=%v, want %v", selected.ID, selected.Writes, want)
		}
	}
}

// A write-only fault is confirmed within MinSamples write intervals plus one
// interval of the failure starting: the detection cost of the longer floor.
func TestWriteOnlyFaultIsConfirmedWithinMinSamplesWriteIntervals(t *testing.T) {
	workload := mixedWorkload(0.01, 1, 1)
	slo := workload.ScenarioSLO("write-00")
	interval := workload.ScenarioArrival("write-00")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	failure := 7 * time.Second
	var samples []Sample
	for at := time.Duration(0); at < 10*time.Minute; at += interval {
		outcome := OutcomeOK
		if at >= failure {
			outcome = OutcomeError
		}
		samples = append(samples, Sample{At: start.Add(at), Outcome: outcome})
		if verdict := Evaluate("write-00", slo, samples, start.Add(at)); !verdict.Healthy {
			if limit := failure + time.Duration(slo.MinSamples+1)*interval; at > limit {
				t.Fatalf("write fault at %s confirmed at %s, later than %s", failure, at, limit)
			}
			return
		}
	}
	t.Fatal("a failing write scenario was never confirmed")
}

func TestWriteSampleIntervalIsBounded(t *testing.T) {
	workload := mixedWorkload(1, 1, 1)
	workload.WriteSampleInterval = Duration(-time.Second)
	if err := workload.Validate(); err == nil {
		t.Fatal("negative writeSampleInterval accepted")
	}
	workload.WriteSampleInterval = Duration(MaxWriteSampleInterval + time.Second)
	if err := workload.Validate(); err == nil {
		t.Fatal("writeSampleInterval over the maximum accepted")
	}
	workload.WriteSampleInterval = Duration(time.Minute)
	if err := workload.Validate(); err != nil {
		t.Fatal(err)
	}
}
