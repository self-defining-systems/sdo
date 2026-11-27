package traffic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// failingServer answers every request with a 500 after latency of simulated time.
type failingServer struct {
	clock   *simClock
	latency time.Duration
}

func (s failingServer) Do(request *http.Request) (*http.Response, error) {
	s.clock.advance(s.latency)
	return &http.Response{StatusCode: 500, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("down")), Request: request}, nil
}

// The estimate's confirmation part is the time the real confirmer and the real
// confirmation predicate take: the confirming samples are taken by the
// engine's confirmer on simulated time, and the failure counts as confirmed at
// the end of the first sample after which ConfirmedFailures names it.
func TestConfirmationDelayIsWhatTheConfirmerAndThePredicateTake(t *testing.T) {
	for _, latency := range []time.Duration{time.Millisecond, 50 * time.Millisecond, 900 * time.Millisecond, 1500 * time.Millisecond, 3 * time.Second, 5 * time.Second} {
		t.Run(latency.String(), func(t *testing.T) {
			clock := newSimClock()
			workload := probeWorkload("read").WithDefaults()
			workload.Timeout, workload.IterationTimeout = Duration(MaxTimeout), Duration(MaxIterationTimeout)
			catalog := Catalog{{ID: "read", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Steps: []Step{{Name: "get", Endpoint: GET("/items", nil)}}}}
			engine, err := NewEngine(workload, catalog, failingServer{clock, latency}, func(Target) string { return "http://web:80" }, clock)
			if err != nil {
				t.Fatal(err)
			}
			_, first := engine.Iterate(context.Background(), 0)
			slo := workload.ScenarioSLO("read")
			if !first.failed(slo) {
				t.Fatalf("first sample did not fail: %+v", first)
			}
			var confirming []Sample
			c := &confirmer{engine: engine, slots: make(chan struct{}, MaxInFlight), active: map[string]bool{}, next: 1 << 31,
				emit: func(_ string, s Sample) { confirming = append(confirming, s) }}
			c.run(context.Background(), "read", first, slo)

			window := Window{Workload: workload}
			observed := ScenarioObservations{Scenario: Descriptor{ID: "read", Target: catalog[0].Target}, Qualified: true, UserTest: true}
			for i := 3; i > 0; i-- {
				observed.Samples = append(observed.Samples, Sample{At: first.At.Add(-time.Duration(i) * time.Second), Outcome: OutcomeOK})
			}
			observed.Samples = append(observed.Samples, first)
			var confirmedAt time.Duration
			for i := 0; i <= len(confirming) && confirmedAt == 0; i++ {
				observed.Confirming = confirming[:i]
				window.ObservedAt = first.end()
				if i > 0 {
					window.ObservedAt = confirming[i-1].end()
				}
				window.Scenarios = []ScenarioObservations{observed}
				if len(ConfirmedFailures(map[string]Window{"w": window})) > 0 {
					confirmedAt = window.ObservedAt.Sub(first.At)
				}
			}
			if confirmedAt == 0 {
				t.Fatalf("never confirmed with %d confirming samples", len(confirming))
			}
			if got := confirmationDelay(latency); got != confirmedAt {
				t.Fatalf("confirmationDelay(%s) = %s, the confirmer and predicate took %s", latency, got, confirmedAt)
			}
		})
	}
}

// The wait for a scenario's next sample is read from its own sampling plan:
// a scenario sampled once per cycle waits up to the cycle (half on average);
// extra weighted slots shorten it; a Poisson arrival waits a mean gap on
// average and at most the four mean gaps a gap is capped at.
func TestExpectedDetectionWaitsForTheScenarioTurnInItsPlan(t *testing.T) {
	healthy := 100 * time.Millisecond
	confirm := confirmationDelay(healthy) + observeDelay
	once := Workload{Name: "home", Purpose: PurposeHealthProbe, SampleInterval: Duration(30 * time.Second),
		Scenarios: []WorkloadScenario{{ID: "a", Weight: 1}, {ID: "b", Weight: 1}, {ID: "c", Weight: 1}}}.WithDefaults()
	got := once.ExpectedDetection("a", healthy)
	if got.Every != 30*time.Second || got.Typical != 15*time.Second+confirm {
		t.Fatalf("once per 30 s: %+v, want every 30s, typical %s", got, 15*time.Second+confirm)
	}
	worstFail := min(healthy+once.Timeout.Duration(), once.IterationTimeout.Duration())
	if want := 30*time.Second + confirmationDelay(worstFail) + observeDelay; got.Worst != want {
		t.Fatalf("worst = %s, want %s", got.Worst, want)
	}

	weighted := once
	weighted.RatePerSecond = 0.2 // 6 slots per 30 s: a, b, c and three extra by weight
	weighted.Scenarios = []WorkloadScenario{{ID: "a", Weight: 4}, {ID: "b", Weight: 1}, {ID: "c", Weight: 1}}
	weighted = weighted.WithDefaults()
	if fast, slow := weighted.ExpectedDetection("a", healthy), weighted.ExpectedDetection("b", healthy); fast.Worst >= slow.Worst || fast.Typical >= slow.Typical {
		t.Fatalf("a scenario with more slots is noticed sooner: a %+v, b %+v", fast, slow)
	}

	poisson := once
	poisson.Arrival = ArrivalPoisson
	p := poisson.ExpectedDetection("a", healthy)
	if p.Typical != 30*time.Second+confirm || p.Worst != 120*time.Second+confirmationDelay(worstFail)+observeDelay {
		t.Fatalf("poisson: %+v", p)
	}

	if (Workload{Name: "x", Purpose: PurposeJourney, Scenarios: []WorkloadScenario{{ID: "a", Weight: 1}}}).ExpectedDetection("a", healthy) != (DetectionEstimate{}) {
		t.Fatal("only a health probe notices")
	}
	if once.ExpectedDetection("missing", healthy) != (DetectionEstimate{}) {
		t.Fatal("a scenario the workload does not schedule is never noticed")
	}
	broken := once
	broken.Scenarios = append([]WorkloadScenario(nil), once.Scenarios...)
	broken.Scenarios[0].KnownBroken = &KnownBroken{}
	if broken.ExpectedDetection("a", healthy) != (DetectionEstimate{}) {
		t.Fatal("a known-broken scenario confirms nothing")
	}
}
