package traffic

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// failure is one failing sample of a test: when it starts, how long it takes,
// and whether the application answered it.
type failure struct {
	at, latency time.Duration
	answered    bool
}

// failingWindow is a health probe whose scenarios passed at -3, -2 and -1 s
// and then failed as given, observed when the last sample ended.
func failingWindow(name string, failures map[string][]failure) Window {
	base := time.Unix(1000, 0)
	window := Window{Workload: Workload{Name: name, Purpose: PurposeHealthProbe}, ObservedAt: base}
	for _, id := range []string{"list", "read"} {
		window.Workload.Scenarios = append(window.Workload.Scenarios, WorkloadScenario{ID: id, Weight: 1})
		observed := ScenarioObservations{Scenario: Descriptor{ID: id, Target: Target{Service: "web", Port: 80}}, Qualified: true}
		for i := -3; i < 0; i++ {
			observed.Samples = append(observed.Samples, Sample{At: base.Add(time.Duration(i) * time.Second), Outcome: OutcomeOK, Status: 200})
		}
		for _, f := range failures[id] {
			sample := Sample{At: base.Add(f.at), Latency: f.latency, Outcome: OutcomeError, Answered: f.answered}
			if f.answered {
				sample.Status = 500
			} else if f.latency > 0 {
				sample.Outcome = OutcomeTimeout
			}
			observed.Samples = append(observed.Samples, sample)
			if end := sample.end(); end.After(window.ObservedAt) {
				window.ObservedAt = end
			}
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	window.Workload = window.Workload.WithDefaults()
	return window
}

func TestAFailureIsConfirmedByTwoSamples(t *testing.T) {
	ms := time.Millisecond
	wrong := func(at time.Duration) failure { return failure{at, 100 * ms, true} }
	refused := func(at time.Duration) failure { return failure{at, 0, false} }
	hang := func(at, latency time.Duration) failure { return failure{at, latency, false} }
	for _, tc := range []struct {
		name       string
		list, read []failure
		want       int
	}{
		{"one flapping failure", []failure{wrong(0)}, nil, 0},
		{"wrong answer twice", []failure{wrong(0), wrong(1100 * ms)}, nil, 1},
		{"wrong answer in two scenarios", []failure{wrong(0)}, []failure{wrong(500 * ms)}, 2},
		{"wrong answers in two scenarios too far apart", []failure{wrong(0)}, []failure{wrong(16 * time.Second)}, 0},
		{"one long hang", []failure{hang(0, 10*time.Second)}, nil, 0},
		{"hang then hang", []failure{hang(0, 5*time.Second), hang(6*time.Second, 5*time.Second)}, nil, 1},
		{"hang in two scenarios", []failure{hang(0, 5*time.Second)}, []failure{hang(1*time.Second, 5*time.Second)}, 2},
		{"3 s stall, refused", []failure{refused(0), refused(1000 * ms), refused(2900 * ms)}, []failure{refused(100 * ms), refused(2900 * ms)}, 0},
		{"3 s stall, dial timeouts in both", []failure{hang(0, time.Second), hang(2*time.Second, time.Second)}, []failure{hang(2900*ms, time.Second)}, 0},
		{"refused for 4 s", []failure{refused(0), refused(1000 * ms), refused(4000 * ms)}, nil, 1},
		{"wrong answer then refused", []failure{wrong(0), refused(1100 * ms)}, nil, 0},
		{"wrong answer then refused in another scenario", []failure{wrong(0)}, []failure{refused(500 * ms)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := failingWindow("probe-read", map[string][]failure{"list": tc.list, "read": tc.read})
			if got := holdFindings(t, window); len(got) != tc.want {
				t.Fatalf("findings = %d, want %d", len(got), tc.want)
			}
		})
	}
}

// A success between two failures, or none before them, confirms nothing: that
// failure is the SLO's. A success that began before the failure refutes nothing.
func TestConfirmationNeedsAFailingRunAfterASuccess(t *testing.T) {
	base := time.Unix(1000, 0)
	at := func(s float64, outcome Outcome) Sample {
		return Sample{At: base.Add(time.Duration(s * float64(time.Second))), Outcome: outcome, Answered: outcome == OutcomeError}
	}
	o, x, inv := OutcomeOK, OutcomeError, OutcomeInvalid
	for _, tc := range []struct {
		name    string
		samples []Sample
		want    int
	}{
		{"success between", []Sample{at(-2, o), at(0, x), at(1, o), at(2, x)}, 0},
		{"recovered", []Sample{at(-2, o), at(0, x), at(1, x), at(2, o)}, 0},
		{"no success held", []Sample{at(0, x), at(1, x), at(2, x)}, 0},
		{"a 429 between says nothing", []Sample{at(-2, o), at(-1, o), at(0, x), at(1, inv), at(2, x)}, 1},
		{"an earlier success recorded late", []Sample{at(-2, o), at(0, x), at(-1, o), at(1, x)}, 1},
		{"a later success under a hang recorded last", []Sample{at(-2, o), at(1, o), at(0, x), at(2, x)}, 0},
		{"too few samples to judge", []Sample{at(-40, o), at(0, x), at(1, x)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := failingWindow("probe-read", nil)
			window.Scenarios[0].Samples, window.ObservedAt = tc.samples, base.Add(3*time.Second)
			if got := ConfirmedFailures(map[string]Window{"probe-read": window}); len(got) != tc.want {
				t.Fatalf("confirmed = %v, want %d", got, tc.want)
			}
		})
	}
}

type windowsContext struct {
	namespacedWindow
	windows map[string]Window
}

func (c windowsContext) TrafficWindows() map[string]Window { return c.windows }

// Two scenarios of different workloads confirm each other, each reported by
// its own detector; one that never passed, is known broken or belongs to
// another kind of workload confirms nothing.
func TestScenariosOfTwoWorkloadsConfirmEachOther(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Window)
		want   int
	}{
		{"both failing", func(*Window) {}, 1},
		{"peer never passed", func(w *Window) { w.Scenarios[1].Qualified = false }, 0},
		{"peer known broken", func(w *Window) {
			w.Workload.Scenarios[1].KnownBroken = &KnownBroken{}
		}, 0},
		{"peer is not a health probe", func(w *Window) { w.Workload.Purpose = PurposeJourney }, 0},
		{"peer not loaded", func(w *Window) { w.LoadError = "prober unavailable" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mine := failingWindow("probe-read", map[string][]failure{"list": {{0, 0, true}}})
			peer := failingWindow("probe-write", map[string][]failure{"read": {{time.Second, 0, true}}})
			tc.change(&peer)
			ctx := windowsContext{namespacedWindow{windowContext{window: mine}}, map[string]Window{"probe-read": mine, "probe-write": peer}}
			detector := NewDetector(sdk.DetectorSpec{ID: "traffic-probe-read", Class: sdk.DetectorClassHealth}, "probe-read")
			findings, err := detector.Detect(t.Context(), ctx)
			if err != nil || len(findings) != tc.want {
				t.Fatalf("findings = %v (%v), want %d", findings, err, tc.want)
			}
		})
	}
}

// Answered is set only by a complete answer the check rejected, so an earlier
// step's status never makes a transport error look answered.
func TestAnsweredMarksAnAnswerTheCheckRejected(t *testing.T) {
	scenario := Scenario{ID: "read", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Steps: []Step{
		{Name: "first", Endpoint: GET("/first", nil)}, {Name: "second", Endpoint: GET("/second", nil).Contains("right")}}}
	for body, want := range map[string]bool{"wrong": true, "": false} {
		clock := newSimClock()
		engine, err := NewEngine(probeWorkload("read"), Catalog{scenario}, doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/second" && body == "" {
				return nil, errors.New("connection reset")
			}
			return (&simServer{clock: clock, handle: func(time.Time, *http.Request) (int, string) { return 200, body }}).Do(r)
		}), func(Target) string { return "http://web:80" }, clock)
		if err != nil {
			t.Fatal(err)
		}
		if _, sample := engine.Iterate(context.Background(), 0); sample.Outcome != OutcomeError || sample.Answered != want {
			t.Fatalf("body %q: %+v", body, sample)
		}
	}
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }
