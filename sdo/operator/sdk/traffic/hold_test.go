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

// A partial failure (a success between its failing samples) waits for the
// hold, which counts from the first failing sample in the window: once the
// failing samples span it, the SLO violation is reported on that evaluation,
// not a further hold later.
func TestHoldCountsFromTheFirstFailingSample(t *testing.T) {
	ok := seconds(-3, -2, -1, 2, 6)
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

// At the default workload cadence a scenario is sampled every second or
// faster, so the five-sample SLO window spans only a few seconds. A fault that
// has failed every second sample for longer than the hold must still fire: the hold
// counts the failure episode, not just the samples the SLO window keeps.
func TestHoldCountsTheFailureEpisodeBeyondTheSLOWindow(t *testing.T) {
	var ok, fail []time.Duration
	for i := 1; i <= 6; i++ {
		ok = append(ok, -time.Duration(i)*750*time.Millisecond)
	}
	for at := time.Duration(0); at <= 8*time.Second; at += 1500 * time.Millisecond {
		fail = append(fail, at)
		ok = append(ok, at+750*time.Millisecond)
	}
	if got := holdFindings(t, holdWindow(ok, fail)); len(got) != 0 {
		t.Fatalf("a failure episode of 8s fired before the hold: %+v", got)
	}
	fail = append(fail, 9*time.Second)
	if got := holdFindings(t, holdWindow(ok, fail)); len(got) != 1 {
		t.Fatalf("a failure episode of 9s did not fire: %+v", got)
	}
}

// An old failure does not join a new episode: it ends once a full SLO window
// of samples passed.
func TestHoldEpisodeEndsAfterAFullWindowOfSuccesses(t *testing.T) {
	var ok []time.Duration
	for at := -20 * time.Second; at < 6*time.Second; at += time.Second {
		ok = append(ok, at)
	}
	ok = append(ok, 7*time.Second, 9*time.Second)
	fail := []time.Duration{-12 * time.Second, 6 * time.Second, 8 * time.Second, 10 * time.Second}
	// ok samples at -12s coincide with the failure; drop that one.
	filtered := ok[:0]
	for _, at := range ok {
		if at != -12*time.Second {
			filtered = append(filtered, at)
		}
	}
	if got := holdFindings(t, holdWindow(filtered, fail)); len(got) != 0 {
		t.Fatalf("an old failure joined a new episode across a full window of successes: %+v", got)
	}
}
