package traffic

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// holdWindow is one health-probe scenario whose samples are okAt (OK) and
// failAt (HTTP 500), observed at the latest sample.
func holdWindow(okAt, failAt []time.Duration) Window {
	base := time.Unix(1000, 0)
	var samples []Sample
	for _, d := range okAt {
		samples = append(samples, Sample{At: base.Add(d), Outcome: OutcomeOK, Status: 200})
	}
	var last time.Duration
	for _, d := range failAt {
		samples = append(samples, Sample{At: base.Add(d), Outcome: OutcomeError, Status: 500, Error: "HTTP 500"})
	}
	for _, s := range samples {
		last = max(last, s.At.Sub(base))
	}
	// Samples are oldest first.
	for i := range samples {
		for j := i + 1; j < len(samples); j++ {
			if samples[j].At.Before(samples[i].At) {
				samples[i], samples[j] = samples[j], samples[i]
			}
		}
	}
	workload := Workload{Name: "probe-read", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{{ID: "read", Weight: 1}}}.WithDefaults()
	return Window{
		Workload:   workload,
		ObservedAt: base.Add(last),
		Scenarios: []ScenarioObservations{{
			Scenario: Descriptor{ID: "read", Target: Target{Service: "web", Port: 80}},
			Samples:  samples, Qualified: true,
		}},
	}
}

func holdFindings(t *testing.T, window Window) []sdk.Finding {
	t.Helper()
	detector := NewDetector(sdk.DetectorSpec{ID: "traffic-probe-read", Class: sdk.DetectorClassHealth}, "probe-read")
	findings, err := detector.Detect(t.Context(), namespacedWindow{windowContext{window: window}})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func seconds(values ...int) []time.Duration {
	out := make([]time.Duration, len(values))
	for i, v := range values {
		out[i] = time.Duration(v) * time.Second
	}
	return out
}

// The hold counts from the first failing sample in the window: once the
// failing samples span it, the SLO violation is reported on that evaluation,
// not a further hold later.
func TestHoldCountsFromTheFirstFailingSample(t *testing.T) {
	ok := seconds(-3, -2, -1)
	if got := holdFindings(t, holdWindow(ok, seconds(0, 4, 8))); len(got) != 0 {
		t.Fatalf("failing samples spanning 8s fired before the hold: %+v", got)
	}
	if got := holdFindings(t, holdWindow(ok, seconds(0, 4, 9))); len(got) != 1 {
		t.Fatalf("failing samples spanning 9s did not fire: %+v", got)
	}
}

// A data-plane stall of a few seconds fails samples close together and must
// not fire, however many of them the window holds.
func TestHoldIgnoresAShortStall(t *testing.T) {
	if got := holdFindings(t, holdWindow(seconds(-3, -2, -1), seconds(0, 1, 2, 3))); len(got) != 0 {
		t.Fatalf("a 3s stall fired: %+v", got)
	}
}

// At the low-volume cadence of one read every 10s (maxAge 30s keeps four
// samples) a 3s stall fails at most one sample, so it never fires. Two
// failing samples 10s apart already span the hold, so the hold adds no delay
// of its own: the incident rule fires on the second failing sample, 10s after
// the first, where counting from the first violating evaluation waited 9s more.
func TestHoldAtOneSamplePerTenSeconds(t *testing.T) {
	ok := seconds(-20, -10)
	if got := holdFindings(t, holdWindow(ok, seconds(0))); len(got) != 0 {
		t.Fatalf("one failing sample fired: %+v", got)
	}
	if got := holdFindings(t, holdWindow(ok, seconds(0, 10))); len(got) != 1 {
		t.Fatalf("two failing samples 10s apart did not fire on the second: %+v", got)
	}
}

// A spec that sets its own MinDuration is held by the finding tracker, not here.
func TestExplicitMinDurationIsNotHeldByTheDetector(t *testing.T) {
	detector := NewDetector(sdk.DetectorSpec{
		ID: "traffic-probe-read", Class: sdk.DetectorClassHealth,
		Persistence: sdk.PersistencePolicy{MinDuration: 2 * time.Second},
	}, "probe-read")
	findings, err := detector.Detect(t.Context(), namespacedWindow{windowContext{window: holdWindow(seconds(-3, -2, -1), seconds(0, 1, 2))}})
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings=%+v err=%v, want one", findings, err)
	}
}
