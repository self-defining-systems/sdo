package traffic

import (
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// failure is one failing sample of a test: when it starts and how long it takes.
type failure struct{ at, latency time.Duration }

// failingWindow is a health probe whose scenarios passed at -3, -2 and -1 s
// and then failed as given, observed when the last sample ended.
func failingWindow(name string, failures map[string][]failure) Window {
	base := time.Unix(1000, 0)
	window := Window{Workload: Workload{Name: name, Purpose: PurposeHealthProbe}, ObservedAt: base}
	for _, id := range []string{"list", "read"} {
		window.Workload.Scenarios = append(window.Workload.Scenarios, WorkloadScenario{ID: id, Weight: 1})
		observed := ScenarioObservations{Scenario: Descriptor{ID: id, Target: Target{Service: "web", Port: 80}}, Qualified: true, UserTest: true}
		for i := -3; i < 0; i++ {
			observed.Samples = append(observed.Samples, Sample{At: base.Add(time.Duration(i) * time.Second), Outcome: OutcomeOK, Status: 200})
		}
		for i, f := range failures[id] {
			sample := Sample{At: base.Add(f.at), Latency: f.latency, Outcome: OutcomeError, Status: 500}
			if i == 0 {
				observed.Samples = append(observed.Samples, sample)
			} else {
				observed.Confirming = append(observed.Confirming, sample)
			}
			if end := sample.end(); end.After(window.ObservedAt) {
				window.ObservedAt = end
			}
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	window.Workload = window.Workload.WithDefaults()
	return window
}

func TestAFailureIsConfirmedByTwoSamplesFourSecondsApart(t *testing.T) {
	const s, ms = time.Second, time.Millisecond
	for _, tc := range []struct {
		name       string
		list, read []failure
		want       int
	}{
		{"one flapping failure", []failure{{0, 0}}, nil, 0},
		{"one long hang", []failure{{0, 10 * s}}, nil, 0},
		{"twice in 1.1 s", []failure{{0, 100 * ms}, {1100 * ms, 100 * ms}}, nil, 0},
		{"three times in 4 s", []failure{{0, 0}, {1 * s, 0}, {4 * s, 0}}, nil, 1},
		{"hang then hang", []failure{{0, 5 * s}, {6 * s, 5 * s}}, nil, 1},
		{"two scenarios within 0.5 s", []failure{{0, 0}}, []failure{{500 * ms, 0}}, 0},
		{"two scenarios 4 s apart", []failure{{0, 0}}, []failure{{4 * s, 0}}, 2},
		{"two scenarios 4 s apart, later one first", []failure{{4 * s, 0}}, []failure{{0, 0}}, 2},
		{"two scenarios too far apart", []failure{{0, 0}}, []failure{{16 * s, 0}}, 0},
		{"3 s stall of both, lost requests", []failure{{0, 2 * s}}, []failure{{2900 * ms, 2 * s}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := failingWindow("probe-read", map[string][]failure{"list": tc.list, "read": tc.read})
			if got := holdFindings(t, window); len(got) != tc.want {
				t.Fatalf("findings = %d, want %d", len(got), tc.want)
			}
		})
	}
}

// Confirming samples are evidence of a failure only: no SLO verdict reads
// them, whatever they say, while a confirming success ends the failing run.
func TestConfirmingSamplesNeverEnterTheSLOWindow(t *testing.T) {
	window := failingWindow("probe-read", map[string][]failure{"list": {{0, 0}}})
	want := Judge(window)
	for _, outcome := range []Outcome{OutcomeOK, OutcomeError} {
		for i := range 6 {
			window.Scenarios[0].Confirming = append(window.Scenarios[0].Confirming, Sample{At: time.Unix(1000+int64(i), 0), Outcome: outcome})
		}
		if got := Judge(window); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s confirming samples changed the verdict: %+v, want %+v", outcome, got, want)
		}
	}
	failing := failingWindow("probe-read", map[string][]failure{"list": {{0, 0}, {1 * time.Second, 0}, {4 * time.Second, 0}}})
	failing.Scenarios[0].Confirming[1].Outcome = OutcomeOK
	if got := ConfirmedFailures(map[string]Window{"probe-read": failing}); len(got) != 0 {
		t.Fatalf("confirmed across a confirming success: %v", got)
	}
}

// A success between two failures, or none before them, confirms nothing: that
// failure is the SLO's. A success that began before the failure refutes nothing.
func TestConfirmationNeedsAFailingRunAfterASuccess(t *testing.T) {
	base := time.Unix(1000, 0)
	at := func(s float64, outcome Outcome) Sample {
		return Sample{At: base.Add(time.Duration(s * float64(time.Second))), Outcome: outcome}
	}
	o, x, inv := OutcomeOK, OutcomeError, OutcomeInvalid
	for _, tc := range []struct {
		name    string
		samples []Sample
		want    int
	}{
		{"success between", []Sample{at(-2, o), at(0, x), at(1, o), at(5, x)}, 0},
		{"recovered", []Sample{at(-2, o), at(0, x), at(4, x), at(5, o)}, 0},
		{"no success held", []Sample{at(0, x), at(2, x), at(4, x)}, 0},
		{"a 429 between says nothing", []Sample{at(-2, o), at(-1, o), at(0, x), at(1, inv), at(4, x)}, 1},
		{"an earlier success recorded late", []Sample{at(-2, o), at(0, x), at(-1, o), at(4, x)}, 1},
		{"a later success under a hang recorded last", []Sample{at(-2, o), at(1, o), at(0, x), at(4, x)}, 0},
		{"too few samples to judge", []Sample{at(-40, o), at(0, x), at(4, x)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			window := failingWindow("probe-read", nil)
			window.Scenarios[0].Samples, window.ObservedAt = tc.samples, base.Add(5*time.Second)
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

// Two user tests of different workloads confirm each other, each reported by
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
		{"peer is not a user test", func(w *Window) { w.Scenarios[1].UserTest = false }, 0},
		{"peer not loaded", func(w *Window) { w.LoadError = "prober unavailable" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mine := failingWindow("probe-read", map[string][]failure{"list": {{0, 0}}})
			peer := failingWindow("probe-write", map[string][]failure{"read": {{4 * time.Second, 0}}})
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

func TestConfirmationUsesScenarioLatencyOverride(t *testing.T) {
	window := failingWindow("probe-read", map[string][]failure{"list": {{0, time.Second}, {4 * time.Second, time.Second}}})
	window.Workload.SLO.MaxLatency = Duration(2 * time.Second)
	window.Workload.Scenarios[0].SLO = &SLO{MaxLatency: Duration(999 * time.Millisecond)}
	observed := &window.Scenarios[0]
	observed.Samples[len(observed.Samples)-1].Outcome = OutcomeOK
	observed.Confirming[0].Outcome = OutcomeOK
	if got := ConfirmedFailures(map[string]Window{"probe-read": window}); got["probe-read/list"] == "" {
		t.Fatalf("slow samples did not confirm against the scenario override: %v", got)
	}
	observed.Confirming[0].Latency = 999 * time.Millisecond
	if got := ConfirmedFailures(map[string]Window{"probe-read": window}); len(got) != 0 {
		t.Fatalf("a sample at the latency limit confirmed a failure: %v", got)
	}
}
