package traffic

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// heavyWorkload is a health probe shaped like a designed one: a few
// scenarios carry most of the weight and four have weight 1, at a rate where a
// weight-1 scenario used to be sampled once a minute.
func heavyWorkload(rate float64, weights ...int) (Workload, Catalog) {
	workload := Workload{
		APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-call", Purpose: PurposeHealthProbe, RatePerSecond: rate,
		SLO: SLO{Window: 6, MinSamples: 3, MaxAge: Duration(60 * time.Second), MaxErrorRate: 0.5},
	}
	var catalog Catalog
	for index, weight := range weights {
		scenario := laterScenario()
		scenario.ID = fmt.Sprintf("scenario-%02d", index)
		catalog = append(catalog, scenario)
		workload.Scenarios = append(workload.Scenarios, WorkloadScenario{ID: scenario.ID, Weight: weight})
	}
	return workload.WithDefaults(), catalog
}

var heavyWeights = []int{20, 20, 6, 3, 3, 1, 1, 1, 1}

func scenarioTimes(plan SamplingPlan, horizon time.Duration) map[string][]time.Duration {
	times := map[string][]time.Duration{}
	for iteration := 0; ; iteration++ {
		at := time.Duration(float64(iteration) / plan.Rate * float64(time.Second))
		if at > horizon {
			return times
		}
		id := plan.Schedule[iteration%len(plan.Schedule)]
		times[id] = append(times[id], at)
	}
}

// Every scenario of a health probe is sampled at least once per
// MinSampleInterval whatever its weight, including a weight-1 scenario among
// heavier ones at a rate that used to leave it one sample a minute.
func TestHealthProbeSamplesEveryScenarioAtTheFloor(t *testing.T) {
	for _, rate := range []float64{0.1, 0.4, 0.9333, 1.5, 4} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			workload, _ := heavyWorkload(rate, heavyWeights...)
			plan := workload.SamplingPlan()
			for _, scenario := range workload.Scenarios {
				times := scenarioTimes(plan, 10*time.Minute)[scenario.ID]
				if len(times) < 2 {
					t.Fatalf("%s sampled %d times in 10 minutes", scenario.ID, len(times))
				}
				for i := 1; i < len(times); i++ {
					if gap := times[i] - times[i-1]; gap > MinSampleInterval+time.Millisecond {
						t.Fatalf("%s: samples %s apart, floor is %s (plan rate %.3f/s)", scenario.ID, gap, MinSampleInterval, plan.Rate)
					}
				}
				if arrival := workload.ScenarioArrival(scenario.ID); arrival == 0 || arrival > MinSampleInterval+time.Millisecond {
					t.Fatalf("%s: ScenarioArrival %s exceeds the floor", scenario.ID, arrival)
				}
			}
			if plan.Raised != (rate*MinSampleInterval.Seconds() < float64(len(heavyWeights))) {
				t.Fatalf("rate %.4f: Raised=%v", rate, plan.Raised)
			}
			if !plan.Raised && plan.Rate != rate {
				t.Fatalf("plan rate %.3f changed a rate that already meets the floor (%.3f)", plan.Rate, rate)
			}
		})
	}
}

// Above the floor, weight still shapes the samples.
func TestWeightShapesSamplesAboveTheFloor(t *testing.T) {
	workload, _ := heavyWorkload(4, heavyWeights...)
	counts := map[string]int{}
	for _, id := range workload.SamplingPlan().Schedule {
		counts[id]++
	}
	if counts["scenario-00"] <= counts["scenario-08"] || counts["scenario-08"] < 1 {
		t.Fatalf("weight 20 got %d samples per cycle, weight 1 got %d", counts["scenario-00"], counts["scenario-08"])
	}
}

// A failing weight-1 scenario is confirmed by the 3-of-6 rule within three
// floor intervals of the failure starting; at one sample a minute it took up
// to 60 s for the first failing sample and two more minutes for the rest.
func TestFailureIsConfirmedWithinThreeFloorIntervals(t *testing.T) {
	workload, _ := heavyWorkload(0.9333, heavyWeights...)
	plan := workload.SamplingPlan()
	slo := workload.ScenarioSLO("scenario-08")
	start := time.Unix(0, 0)
	for phase := time.Duration(0); phase < MinSampleInterval; phase += 250 * time.Millisecond {
		failure := 5*time.Minute + phase
		var samples []Sample
		var confirmed time.Duration
		for _, at := range scenarioTimes(plan, 10*time.Minute)["scenario-08"] {
			outcome := OutcomeOK
			if at >= failure {
				outcome = OutcomeError
			}
			samples = append(samples, Sample{At: start.Add(at), Outcome: outcome})
			if verdict := Evaluate("scenario-08", slo, samples, start.Add(at)); !verdict.Healthy {
				confirmed = at
				break
			}
		}
		if confirmed == 0 || confirmed-failure > 3*MinSampleInterval {
			t.Fatalf("failure at %s confirmed at %s: more than %s later", failure, confirmed, 3*MinSampleInterval)
		}
	}
}

type recordingClock struct {
	mu   sync.Mutex
	now  time.Time
	gaps []time.Duration
	stop context.CancelFunc
	max  int
}

func (c *recordingClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *recordingClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gaps = append(c.gaps, d)
	c.now = c.now.Add(d)
	if len(c.gaps) >= c.max {
		c.stop()
	}
	return make(chan time.Time)
}

// The engine starts iterations at the plan's rate, not the workload's.
func TestEngineRunsAtTheFlooredRate(t *testing.T) {
	workload, catalog := heavyWorkload(0.9333, heavyWeights...)
	ctx, cancel := context.WithCancel(context.Background())
	clock := &recordingClock{now: time.Unix(0, 0), stop: cancel, max: 1}
	engine, err := NewEngine(workload, catalog, clockedDoer{&itemStore{items: map[string]bool{}}, &steppingClock{}}, func(Target) string { return "http://web:80" }, clock)
	if err != nil {
		t.Fatal(err)
	}
	engine.Run(ctx, 0, func(string, Sample) {})
	if got := engine.Rate(); got < float64(len(heavyWeights))/MinSampleInterval.Seconds() {
		t.Fatalf("engine rate %.3f cannot sample %d scenarios per %s", got, len(heavyWeights), MinSampleInterval)
	}
	if len(clock.gaps) != 1 || clock.gaps[0] != time.Duration(float64(time.Second)/engine.Rate()) {
		t.Fatalf("iteration gap %v, want %v", clock.gaps, time.Duration(float64(time.Second)/engine.Rate()))
	}
}

// When the floor for many scenarios exceeds the load cap the probe load says
// so; it never drops a scenario below the floor.
func TestProbeLoadReportsFloorOverTheCap(t *testing.T) {
	small, _ := heavyWorkload(0.4, 1, 1)
	if load := MeasureProbeLoad([]Workload{small}, nil); load.OverCap() || len(load.Raised) != 0 {
		t.Fatalf("a probe already meeting the floor was reported: %+v", load)
	}
	weights := make([]int, 40)
	for i := range weights {
		weights[i] = 1
	}
	big, _ := heavyWorkload(0.5, weights...)
	load := MeasureProbeLoad([]Workload{big}, map[string]int{"scenario-00": 2})
	if !load.OverCap() || len(load.Raised) != 1 || load.Raised[0] != "probe-call" {
		t.Fatalf("40 scenarios at a 10 s floor send 4 requests/s or more over the %.0f/s cap, got %+v", ProbeRateCap, load)
	}
	if plan := big.SamplingPlan(); len(scenarioTimes(plan, time.Minute)) != 40 {
		t.Fatal("the floor was relaxed to meet the cap")
	}
}
