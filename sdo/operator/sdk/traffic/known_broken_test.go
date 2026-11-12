package traffic

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type namespacedWindow struct{ windowContext }

func (namespacedWindow) Namespace() string { return "app" }

type explainedDetector interface {
	Confirmed(sdk.DetectionContext) bool
	ConfirmationReason(sdk.DetectionContext) string
}

// knownBrokenWindow is a probe workload whose "list" scenario passes and whose
// "review" scenario is declared broken at baseline with HTTP 404.
func knownBrokenWindow(review []Sample) Window {
	at := time.Unix(1000, 0)
	var ok []Sample
	for i := range 5 {
		ok = append(ok, Sample{At: at.Add(time.Duration(i) * time.Second), Outcome: OutcomeOK, Status: 200, Latency: 10 * time.Millisecond})
	}
	qualified := false
	for _, s := range review {
		qualified = qualified || s.Outcome == OutcomeOK
	}
	workload := Workload{Name: "probe-call", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{
		{ID: "list", Weight: 1},
		{ID: "review", Weight: 1, KnownBroken: &KnownBroken{Reason: "route returns 404 in the deployed source", FailsWith: []string{"404"}}},
	}}.WithDefaults()
	return Window{
		Workload:   workload,
		ObservedAt: at.Add(5 * time.Second),
		Scenarios: []ScenarioObservations{
			{Scenario: Descriptor{ID: "list", Target: Target{Service: "web", Port: 80}}, Samples: ok, Qualified: true},
			{Scenario: Descriptor{ID: "review", Target: Target{Service: "web", Port: 80}}, Samples: review, Qualified: qualified},
		},
	}
}

func reviewSamples(outcome Outcome, status int) []Sample {
	at := time.Unix(1000, 0)
	var out []Sample
	for i := range 5 {
		out = append(out, Sample{At: at.Add(time.Duration(i) * time.Second), Outcome: outcome, Status: status, Latency: 10 * time.Millisecond, Error: "synthetic"})
	}
	return out
}

func evaluateKnownBroken(t *testing.T, window Window) ([]sdk.Finding, bool, string) {
	t.Helper()
	detector := NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth}, "probe-call")
	ctx := namespacedWindow{windowContext{window: window}}
	findings, err := detector.Detect(t.Context(), ctx)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	explained := detector.(explainedDetector)
	return findings, explained.Confirmed(ctx), explained.ConfirmationReason(ctx)
}

// A scenario declared broken at baseline that keeps failing the declared way
// neither fires nor holds health unconfirmed, and says nothing.
func TestKnownBrokenScenarioFailingAsDeclaredIsNotGating(t *testing.T) {
	findings, confirmed, reason := evaluateKnownBroken(t, knownBrokenWindow(reviewSamples(OutcomeError, 404)))
	if len(findings) != 0 {
		t.Fatalf("a known-broken scenario fired: %+v", findings)
	}
	if !confirmed {
		t.Fatalf("a known-broken scenario that never succeeded held health unconfirmed: %s", reason)
	}
	if reason != "" {
		t.Fatalf("an unchanged known-broken scenario produced a note: %s", reason)
	}
}

// A known-broken scenario that starts working is surfaced as a non-gating
// note: health stays confirmed and the reason names the scenario.
func TestKnownBrokenScenarioThatStartsWorkingIsANote(t *testing.T) {
	findings, confirmed, reason := evaluateKnownBroken(t, knownBrokenWindow(reviewSamples(OutcomeOK, 200)))
	if len(findings) != 0 || !confirmed {
		t.Fatalf("findings=%+v confirmed=%v, want none and confirmed", findings, confirmed)
	}
	if !strings.Contains(reason, "review") || !strings.Contains(reason, "meets its SLO") {
		t.Fatalf("note = %q, want it to say review now meets its SLO", reason)
	}
}

// A known-broken scenario that fails differently from its declaration is
// surfaced as a non-gating note naming the new failure.
func TestKnownBrokenScenarioFailingDifferentlyIsANote(t *testing.T) {
	findings, confirmed, reason := evaluateKnownBroken(t, knownBrokenWindow(reviewSamples(OutcomeError, 503)))
	if len(findings) != 0 || !confirmed {
		t.Fatalf("findings=%+v confirmed=%v, want none and confirmed", findings, confirmed)
	}
	if !strings.Contains(reason, "review") || !strings.Contains(reason, "503") || !strings.Contains(reason, "404") {
		t.Fatalf("note = %q, want the scenario, the new failure 503 and the declared 404", reason)
	}
}

// The gated scenarios of the same workload still fire and still gate
// confirmation.
func TestKnownBrokenDoesNotHideTheOtherScenarios(t *testing.T) {
	window := knownBrokenWindow(reviewSamples(OutcomeError, 404))
	window.Scenarios[0].Samples = reviewSamples(OutcomeError, 500)
	for i := range window.Scenarios[0].Samples {
		// Spread the failures over longer than the health hold.
		window.Scenarios[0].Samples[i].At = time.Unix(1000, 0).Add(time.Duration(i) * 3 * time.Second)
	}
	window.ObservedAt = time.Unix(1000, 0).Add(12 * time.Second)
	findings, _, _ := evaluateKnownBroken(t, window)
	if len(findings) != 1 || !strings.HasSuffix(findings[0].RuleID, ".list") {
		t.Fatalf("findings = %+v, want exactly the list scenario", findings)
	}
	window.Scenarios[0].Qualified = false
	if _, confirmed, _ := evaluateKnownBroken(t, window); confirmed {
		t.Fatal("an unproven gated scenario was confirmed")
	}
}

func TestKnownBrokenDeclarationValidation(t *testing.T) {
	valid := func() Workload {
		return Workload{
			APIVersion: APIVersion, Kind: WorkloadKind, Name: "probe-call", Purpose: PurposeHealthProbe,
			Scenarios: []WorkloadScenario{{ID: "list"}, {ID: "review", KnownBroken: &KnownBroken{Reason: "returns 404", FailsWith: []string{"404", "timeout"}}}},
		}.WithDefaults()
	}
	if err := valid().Validate(); err != nil {
		t.Fatalf("a valid known-broken declaration was rejected: %v", err)
	}
	cases := map[string]func(*Workload){
		"no reason":        func(w *Workload) { w.Scenarios[1].KnownBroken.Reason = "  " },
		"long reason":      func(w *Workload) { w.Scenarios[1].KnownBroken.Reason = strings.Repeat("x", MaxKnownBrokenReason+1) },
		"no failsWith":     func(w *Workload) { w.Scenarios[1].KnownBroken.FailsWith = nil },
		"unknown failure":  func(w *Workload) { w.Scenarios[1].KnownBroken.FailsWith = []string{"teapot"} },
		"status too large": func(w *Workload) { w.Scenarios[1].KnownBroken.FailsWith = []string{"700"} },
		"every scenario": func(w *Workload) {
			w.Scenarios[0].KnownBroken = &KnownBroken{Reason: "returns 404", FailsWith: []string{"404"}}
		},
		"not a health probe": func(w *Workload) {
			w.Purpose = PurposeVerifyBurst
			w.Duration = Duration(3 * time.Second)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			w := valid()
			mutate(&w)
			if err := w.Validate(); err == nil {
				t.Fatalf("an invalid known-broken declaration (%s) was accepted", name)
			}
		})
	}
}
