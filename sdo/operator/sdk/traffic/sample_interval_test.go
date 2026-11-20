package traffic

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func intervalWorkload(t *testing.T, interval string, scenarios int) Workload {
	t.Helper()
	var ids []string
	for i := 0; i < scenarios; i++ {
		ids = append(ids, fmt.Sprintf(`{"id":"scenario-%02d"}`, i))
	}
	text := fmt.Sprintf(`{"apiVersion":%q,"kind":%q,"name":"probe-call","purpose":"health-probe","sampleInterval":%q,"scenarios":[%s]}`,
		APIVersion, WorkloadKind, interval, strings.Join(ids, ","))
	workload, err := ParseWorkloadJSON([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return workload
}

// Raising the interval brings many scenarios under the probe-load bound that
// the fixed 10 s floor exceeds.
func TestSampleIntervalKeepsManyScenariosUnderTheProbeLoadBound(t *testing.T) {
	steps := map[string]int{}
	for i := 0; i < 18; i++ {
		steps[fmt.Sprintf("scenario-%02d", i)] = 2
	}
	fixed := intervalWorkload(t, "10s", 18)
	if load := MeasureProbeLoad([]Workload{fixed}, steps); !load.OverCap() {
		t.Fatalf("18 scenarios at 10 s should exceed the cap, got %+v", load)
	}
	slow := intervalWorkload(t, "30s", 18)
	load := MeasureProbeLoad([]Workload{slow}, steps)
	if load.OverCap() || len(load.Raised) != 0 {
		t.Fatalf("18 scenarios at 30 s should fit the cap, got %+v", load)
	}
	if want := 2 * 18.0 / 30; load.RequestsPerSecond < want-1e-9 || load.RequestsPerSecond > want+1e-9 {
		t.Fatalf("load %.3f, want %.3f", load.RequestsPerSecond, want)
	}
}

// A broken scenario still fires within minSamples intervals of failing, in
// simulated time, and the detection horizon grows with the interval.
func TestSampleIntervalDetectsWithinPersistenceTimesInterval(t *testing.T) {
	workload := intervalWorkload(t, "30s", 18)
	plan := workload.SamplingPlan()
	slo := workload.ScenarioSLO("scenario-17")
	interval := workload.EffectiveSampleInterval()
	start := time.Unix(0, 0)
	times := scenarioTimes(plan, 30*time.Minute)["scenario-17"]
	for phase := time.Duration(0); phase < interval; phase += time.Second {
		failure := 10*time.Minute + phase
		var samples []Sample
		var confirmed time.Duration
		for _, at := range times {
			outcome := OutcomeOK
			if at >= failure {
				outcome = OutcomeError
			}
			samples = append(samples, Sample{At: start.Add(at), Outcome: outcome})
			if !Evaluate("scenario-17", slo, samples, start.Add(at)).Healthy {
				confirmed = at
				break
			}
		}
		limit := time.Duration(slo.MinSamples+1) * interval
		if confirmed == 0 || confirmed-failure > limit {
			t.Fatalf("failure at %s confirmed at %s, more than %s later", failure, confirmed, limit)
		}
	}
	if fixed := intervalWorkload(t, "10s", 18); workload.DetectionHorizon() <= fixed.DetectionHorizon() {
		t.Fatalf("horizon %s at 30 s should exceed %s at 10 s", workload.DetectionHorizon(), fixed.DetectionHorizon())
	}
}

func TestSampleIntervalValidation(t *testing.T) {
	good := intervalWorkload(t, "30s", 2)
	if got := good.SLO.MaxAge.Duration(); got < 3*30*time.Second {
		t.Fatalf("default maxAge %s cannot hold minSamples at 30 s", got)
	}
	cases := map[string]func(Workload) Workload{
		"below the minimum": func(w Workload) Workload { w.SampleInterval = Duration(5 * time.Second); return w },
		"above the maximum": func(w Workload) Workload { w.SampleInterval = Duration(MaxSampleInterval + time.Second); return w },
		"maxAge too short":  func(w Workload) Workload { w.SLO.MaxAge = Duration(40 * time.Second); return w },
		"not a probe": func(w Workload) Workload {
			w.Purpose = PurposeVerifyBurst
			w.Duration = Duration(5 * time.Second)
			return w
		},
	}
	for name, mutate := range cases {
		if err := mutate(good).Validate(); err == nil {
			t.Fatalf("%s: workload accepted", name)
		}
	}
}

// The memory check rejects a probe set that sends more than the cap, naming the
// rate and the way out, instead of leaving the controller to log it.
func TestCheckProbeLoadRejectsAnOverCapProbeSet(t *testing.T) {
	var catalog Catalog
	for i := 0; i < 18; i++ {
		catalog = append(catalog, Scenario{ID: fmt.Sprintf("scenario-%02d", i), Steps: make([]Step, 2)})
	}
	err := CheckProbeLoad([]Workload{intervalWorkload(t, "10s", 18)}, catalog)
	if err == nil {
		t.Fatal("an 18-scenario probe at the 10 s floor was accepted")
	}
	for _, want := range []string{"3.6 requests/s", "3/s cap", "sampleInterval"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q lacks %q", err, want)
		}
	}
	if err := CheckProbeLoad([]Workload{intervalWorkload(t, "30s", 18)}, catalog); err != nil {
		t.Fatalf("a probe set under the cap was rejected: %v", err)
	}
}

// The default rate of a workload that sets none is not the designer's choice:
// only the floor load of sampling each scenario once per interval is checked.
func TestCheckProbeLoadIgnoresTheDefaultRate(t *testing.T) {
	catalog := Catalog{{ID: "scenario-00", Steps: make([]Step, 1)}}
	var workloads []Workload
	for i := 0; i < 3; i++ {
		w := intervalWorkload(t, "10s", 1)
		w.Name, w.RatePerSecond = fmt.Sprintf("probe-%d", i), DefaultRatePerSecond
		workloads = append(workloads, w)
	}
	if err := CheckProbeLoad(workloads, catalog); err != nil {
		t.Fatalf("three single-scenario probes at the default rate were rejected: %v", err)
	}
}
