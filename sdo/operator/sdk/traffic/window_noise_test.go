package traffic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

func TestDefaultLatencyIgnoresIsolatedPausesAndHonorsScenarioOverrides(t *testing.T) {
	for size := 3; size <= traffic.DefaultWindow; size++ {
		for pause := 0; pause < size; pause++ {
			t.Run(fmt.Sprintf("window-%d-pause-%d", size, pause), func(t *testing.T) {
				w := healthWorkload(t)
				values := slowSamples(observedAt, 40*time.Millisecond)[:size]
				values[pause].Latency = 1900 * time.Millisecond
				if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); !v.Evaluated || !v.Healthy || v.SLO.LatencyPercentile != 50 {
					t.Fatalf("one healthy-app pause must not fire: %+v", v)
				}
				w.Scenarios[0].SLO = &traffic.SLO{LatencyPercentile: 90}
				if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); v.Healthy {
					t.Fatalf("explicit per-scenario tail bound must still fire: %+v", v)
				}
			})
		}
	}
}

func TestTimeoutNoiseFloorAcrossWindowSizesAndRateBounds(t *testing.T) {
	for size := 3; size <= 10; size++ {
		for _, rate := range []float64{0.01, 0.1, 0.2} {
			for position := 0; position < size; position++ {
				t.Run(fmt.Sprintf("window-%d-rate-%g-position-%d", size, rate, position), func(t *testing.T) {
					w := healthWorkload(t)
					w.SLO.Window = size
					w.SLO.MaxErrorRate, w.SLO.MaxTimeoutRate = rate, rate
					values := make([]traffic.Sample, size)
					for i := range values {
						values[i] = ok(time.Duration(size-i) * time.Second)
					}
					values[position] = timedOut(time.Duration(size-position) * time.Second)
					slo := w.ScenarioSLO("search")
					v := traffic.Evaluate("search", slo, values, observedAt)
					if !v.Evaluated || !v.Healthy || v.Timeouts != 1 || v.TimeoutRate != 1/float64(size) || len(v.RecentFailures) != 1 {
						t.Fatalf("isolated timeout must remain visible without firing: %+v", v)
					}
					values[(position+1)%size] = timedOut(time.Second)
					if v := traffic.Evaluate("search", slo, values, observedAt); v.Healthy {
						t.Fatalf("repeated timeouts above the rate bound must fire: %+v", v)
					}
					values[position] = failed(time.Second, 503, "upstream error")
					values[(position+1)%size] = ok(time.Second)
					if v := traffic.Evaluate("search", slo, values, observedAt); v.Healthy != (1/float64(size) < rate) {
						t.Fatalf("application errors retain the configured error bound: %+v", v)
					}
				})
			}
		}
	}
}

func TestTimeoutFloorCanBeOverriddenPerScenario(t *testing.T) {
	w := healthWorkload(t)
	w.SLO.MaxErrorRate, w.SLO.MaxTimeoutRate = 0.1, 0.1
	w.SLO.MinTimeouts = 3
	w.Scenarios[0].SLO = &traffic.SLO{MinTimeouts: 1}
	values := []traffic.Sample{ok(3 * time.Second), ok(2 * time.Second), timedOut(time.Second)}
	if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); v.Healthy {
		t.Fatalf("explicit strict scenario must fire on one timeout: %+v", v)
	}
	if v := traffic.Evaluate("login", w.ScenarioSLO("login"), values, observedAt); !v.Healthy {
		t.Fatalf("peer scenario must retain workload noise floor: %+v", v)
	}
	for _, count := range []int{-1, 101} {
		w.Scenarios[0].SLO.MinTimeouts = count
		if err := w.Validate(); err == nil {
			t.Fatalf("invalid timeout floor %d accepted", count)
		}
	}
}

// HTTP producer -> engine samples -> detector, with no hand-authored
// observations at the decision boundary. A hung response times out on the
// injected clock rather than an operating-system deadline.
func TestEngineTimeoutNoiseFeedsDetector(t *testing.T) {
	base := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) }))
	address := base(traffic.Target{})
	clock := &stepClock{now: time.Unix(0, 0)}
	w := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "search"})
	w.Timeout = traffic.Duration(50 * time.Millisecond)
	w.SLO.MaxErrorRate, w.SLO.MaxTimeoutRate = 0.1, 0.1
	e, err := traffic.NewEngine(w, traffic.Catalog{search()}, routedDoer{}, base, clock)
	if err != nil {
		t.Fatal(err)
	}
	var values []traffic.Sample
	for i := 0; i < 5; i++ {
		if i == 3 {
			applications.Store(address, deadlineDoer{t: t, clock: clock})
		}
		_, sample := e.Iterate(context.Background(), uint64(i))
		want := traffic.OutcomeOK
		if i >= 3 {
			want = traffic.OutcomeTimeout
		}
		if sample.Outcome != want {
			t.Fatalf("real request outcome: %+v", sample)
		}
		values = append(values, sample)
		if i < 3 {
			continue
		}
		snapshot := hotelSnapshot(window(w, observed(descriptor("search"), values...)))
		win := snapshot.Traffic["health"]
		win.ObservedAt = sample.At.Add(sample.Latency)
		snapshot.Traffic["health"] = win
		findings := detect(t, snapshot)
		if (len(findings) > 0) != (i == 4) {
			t.Fatalf("timeout count %d: findings %+v", i-2, findings)
		}
	}
}

func TestTimeoutFloorWorkloadJSONContract(t *testing.T) {
	w, err := traffic.ParseWorkloadJSON([]byte(`{
 "apiVersion":"sdo.dev/v1alpha1", "kind":"TrafficWorkload", "name":"health", "purpose":"health-probe",
 "slo":{"minTimeouts":3}, "scenarios":[{"id":"strict","slo":{"minTimeouts":1,"latencyPercentile":90}},{"id":"default"}]
 }`))
	if err != nil {
		t.Fatal(err)
	}
	if w.ScenarioSLO("strict").MinTimeouts != 1 || w.ScenarioSLO("strict").LatencyPercentile != 90 || w.ScenarioSLO("default").MinTimeouts != 3 || w.ScenarioSLO("default").LatencyPercentile != 50 {
		t.Fatalf("effective SLOs: %+v", w)
	}
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := traffic.ParseWorkloadJSON(data)
	if err != nil || roundTrip.ScenarioSLO("strict") != w.ScenarioSLO("strict") {
		t.Fatalf("SLO JSON roundtrip: %s %v", data, err)
	}
}

func TestTimeoutNoiseDoesNotAmplifyAnErrorOrSurviveWindowExpiry(t *testing.T) {
	w := healthWorkload(t)
	w.SLO.MaxErrorRate, w.SLO.MaxTimeoutRate = 0.3, 0.1
	values := []traffic.Sample{timedOut(6 * time.Second), ok(5 * time.Second), failed(4*time.Second, 503, "error"), timedOut(3 * time.Second), ok(2 * time.Second), ok(time.Second)}
	if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); !v.Healthy || v.Timeouts != 1 || v.Errors != 1 {
		t.Fatalf("expired timeout must not corroborate a fresh timeout: %+v", v)
	}
	values = append(values, timedOut(0))
	if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); v.Healthy || v.Timeouts != 2 {
		t.Fatalf("two current timeouts must fire: %+v", v)
	}
	values = append(values, ok(0), ok(0), ok(0), ok(0), ok(0))
	if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); !v.Healthy {
		t.Fatalf("recovery must clear timeout evidence: %+v", v)
	}
}

func TestTimeoutFloorCannotHideACompletelyTimedOutWindow(t *testing.T) {
	for size := 1; size <= 10; size++ {
		for _, floor := range []int{traffic.DefaultMinTimeouts, 100} {
			t.Run(fmt.Sprintf("window-%d-floor-%d", size, floor), func(t *testing.T) {
				w := healthWorkload(t)
				w.SLO.Window, w.SLO.MinSamples, w.SLO.MinTimeouts = size, size, floor
				if err := w.Validate(); err != nil {
					t.Fatal(err)
				}
				values := make([]traffic.Sample, size)
				for i := range values {
					values[i] = timedOut(time.Duration(size-i) * time.Second)
				}
				if v := traffic.Evaluate("search", w.ScenarioSLO("search"), values, observedAt); v.Healthy {
					t.Fatalf("an unreachable timeout floor must not hide a total outage: %+v", v)
				}
			})
		}
	}
}
