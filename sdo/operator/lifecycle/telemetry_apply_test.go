package lifecycle_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/telemetryapply"
)

// fakeTelemetryApplier answers one scripted result per call (the last repeats)
// and records the checkout and how many detector evaluations had run.
type fakeTelemetryApplier struct {
	verifier *fakeVerifier
	results  []error
	applied  telemetryapply.Applied
	calls    []string
	// evaluationsBefore is len(verifier.candidateRoots) at each call.
	evaluationsBefore []int
}

func (f *fakeTelemetryApplier) Apply(_ context.Context, checkout string) (telemetryapply.Applied, error) {
	f.calls = append(f.calls, checkout)
	f.evaluationsBefore = append(f.evaluationsBefore, len(f.verifier.candidateRoots))
	var err error
	if len(f.results) > 0 {
		err = f.results[min(len(f.calls)-1, len(f.results)-1)]
	}
	if err != nil {
		return telemetryapply.Applied{}, err
	}
	return f.applied, nil
}

func telemetryHarness(t *testing.T, applier *fakeTelemetryApplier, verdicts ...lifecycle.HealthVerdict) (*harness, *fakeBroker) {
	t.Helper()
	h, fake := gateHarness(t, verdicts...)
	applier.verifier = h.verifier
	h.coordinator.TelemetryApply = applier
	return h, fake
}

// o3Checks are the accepted report's O3.telemetry checks.
func o3Checks(report *operability.Report) []operability.Check {
	var out []operability.Check
	for _, item := range report.Items {
		for _, check := range item.Checks {
			if check.ID == "O3.telemetry" {
				out = append(out, check)
			}
		}
	}
	return out
}

// The designer's backends are deployed on the staged candidate, after the live
// deployment checks and before the detectors are evaluated, and the accepted
// report records what was applied.
func TestTelemetryBackendsAreAppliedBeforeDetectorsAreEvaluated(t *testing.T) {
	applier := &fakeTelemetryApplier{applied: telemetryapply.Applied{Backends: []telemetryapply.AppliedBackend{{ID: "stack", Recipe: "prometheus", Objects: []string{"Deployment/sdo-prometheus"}}}}}
	h, fake := telemetryHarness(t, applier, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	candidate := fake.candidates[0]
	if len(applier.calls) != 1 || applier.calls[0] != candidate.Path || applier.evaluationsBefore[0] != 0 {
		t.Fatalf("apply calls = %v before %v evaluations; want one call on the staged checkout before any evaluation", applier.calls, applier.evaluationsBefore)
	}
	checks := o3Checks(fake.evidence2[0].Operability)
	if len(checks) != 1 || checks[0].ID != "O3.telemetry" || checks[0].Status != operability.StatusPass || !strings.Contains(checks[0].Detail, "Deployment/sdo-prometheus") {
		t.Fatalf("accepted report O3 checks = %+v; want the pass recording what was applied", checks)
	}
}

func TestNoTelemetryApplierAndNoBackendsLeaveTheReportUnchanged(t *testing.T) {
	h, fake := gateHarness(t, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if checks := o3Checks(fake.evidence2[0].Operability); len(checks) != 0 {
		t.Fatalf("O3 checks = %+v", checks)
	}
	applier := &fakeTelemetryApplier{}
	h, fake = telemetryHarness(t, applier, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if checks := o3Checks(fake.evidence2[0].Operability); len(checks) != 0 {
		t.Fatalf("a candidate with no backends must add no check: %+v", checks)
	}
}

// A backend the policy denies, or that never becomes ready, fails closed: the
// candidate is not accepted, no detector is evaluated against it, and the
// designer gets the typed reason with the file to correct.
func TestTelemetryApplyFailureIsTheDesignersTypedCorrection(t *testing.T) {
	for _, reason := range []telemetryapply.Reason{telemetryapply.ReasonDenied, telemetryapply.ReasonNotReady} {
		denied := &telemetryapply.Error{Reason: reason, Backend: "stack", Path: layout.TelemetryBackendsFile, Detail: "synthetic " + string(reason)}
		applier := &fakeTelemetryApplier{results: []error{denied, nil}}
		h, fake := telemetryHarness(t, applier, clear)
		if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
			t.Fatal(err)
		}
		if h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 2 || fake.discards != 1 || fake.accepts != 1 {
			t.Fatalf("%s: deployer attempts=%d judge runs=%d discards=%d accepts=%d", reason, h.workers.deployerAttempts(), h.workers.judgeRuns, fake.discards, fake.accepts)
		}
		if len(h.verifier.candidateRoots) != 1 {
			t.Fatalf("%s: detectors evaluated %d times; the failed candidate must not be evaluated", reason, len(h.verifier.candidateRoots))
		}
		previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
		if len(previous) != 1 || previous[0].GetRule() != "operability.O3.telemetry" || previous[0].GetPath() != layout.TelemetryBackendsFile ||
			!strings.Contains(previous[0].GetMessage(), "["+string(reason)+"]") || !strings.Contains(previous[0].GetMessage(), "synthetic") {
			t.Fatalf("%s: designer correction = %v", reason, previous)
		}
	}
}

func TestTelemetryApplyInfrastructureFailureIsRetriedWithoutAnAttempt(t *testing.T) {
	applier := &fakeTelemetryApplier{results: []error{errors.New("apiserver unavailable"), nil}}
	h, fake := telemetryHarness(t, applier, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if len(applier.calls) != 2 || h.workers.judgeRuns != 1 || fake.accepts != 1 {
		t.Fatalf("apply calls=%d judge runs=%d accepts=%d", len(applier.calls), h.workers.judgeRuns, fake.accepts)
	}
}
