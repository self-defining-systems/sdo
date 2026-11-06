package lifecycle_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operability"
)

func sameFailure() error {
	return &validate.Failure{Diagnostics: []validate.Diagnostic{
		{Rule: "detector.test", Path: ".sdo/diagnostics/detectors/health/traffic-web/detector.go", Message: "attempt text differs each time"},
		{Rule: "manifest.invalid", Path: ".sdo/diagnostics/detectors/health/traffic-web/manifest.yaml", Message: "x"},
	}}
}

// An agent that returns a proposal the broker rejects with the same rules and
// paths as its previous attempt is not converging: the run ends with a typed
// cause naming the repeated rules, and no further agent turn is spent.
func TestJudgeRepeatingTheSameRejectionEndsAsNotConverging(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.stageErrors = []error{sameFailure(), sameFailure(), sameFailure(), sameFailure(), sameFailure(), sameFailure()}
	_, err := h.coordinator.Run(context.Background(), config())
	var stop *lifecycle.AgentNotConvergingError
	if !errors.As(err, &stop) {
		t.Fatalf("error = %v, want AgentNotConvergingError", err)
	}
	if stop.Role != operability.OwnerDesigner {
		t.Errorf("role = %q, want designer", stop.Role)
	}
	want := []string{"detector.test@.sdo/diagnostics/detectors/health/traffic-web/detector.go", "manifest.invalid@.sdo/diagnostics/detectors/health/traffic-web/manifest.yaml"}
	if !slices.Equal(stop.Repeated, want) {
		t.Errorf("repeated = %v, want %v", stop.Repeated, want)
	}
	if h.workers.judgeRuns != 2 {
		t.Errorf("judge runs = %d, want 2 (the second attempt repeated the first)", h.workers.judgeRuns)
	}
	var opErr *lifecycle.OperabilityError
	if !errors.As(err, &opErr) || opErr.Decision.Kind != operability.Hold || opErr.Decision.Owner != operability.OwnerDesigner {
		t.Fatalf("stop is not a terminal operability decision for the designer: %+v", opErr)
	}
	if opErr.Decision.Checks[0].Cause != operability.CauseAgentNotConverging || !strings.Contains(opErr.Decision.Feedback(), "detector.test") {
		t.Errorf("decision = %s", opErr.Decision.Feedback())
	}
	if err := opErr.Report.Validate(); err != nil {
		t.Errorf("report invalid: %v", err)
	}
	if h.installer.installs != 0 || h.workers.deployerAttempts() != 1 {
		t.Errorf("installs=%d deployer attempts=%d", h.installer.installs, h.workers.deployerAttempts())
	}
}

// Distinct rejections that spend the retry bound also end the run: another
// judge round over the same source would only repeat the budget.
func TestJudgeRetryBudgetExhaustedEndsAsNotConverging(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.stageErrors = distinctFailures("rule.one", "rule.two", "rule.three", "rule.four", "rule.five", "rule.six")
	_, err := h.coordinator.Run(context.Background(), config())
	var stop *lifecycle.AgentNotConvergingError
	if !errors.As(err, &stop) {
		t.Fatalf("error = %v, want AgentNotConvergingError", err)
	}
	if h.workers.judgeRuns != 1+broker.MaxValidationRetries {
		t.Errorf("judge runs = %d, want %d: one round, no second round", h.workers.judgeRuns, 1+broker.MaxValidationRetries)
	}
	for _, rule := range []string{"rule.one", "rule.six"} {
		if !strings.Contains(err.Error(), rule) {
			t.Errorf("error lacks %s: %v", rule, err)
		}
	}
}

// A designer correction that repeats is the O4 discriminator's to attribute,
// and lost workers say nothing about the agent: neither is "not converging".
func TestRepeatedGateCorrectionsAndLostWorkersAreNotConvergenceStops(t *testing.T) {
	h, _ := gateHarness(t, judgeFiring, judgeFiring, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		var stop *lifecycle.AgentNotConvergingError
		if errors.As(err, &stop) {
			t.Fatalf("designer gate corrections were stopped as not converging: %v", err)
		}
	}
}

// A base whose own validation fails is SDO's defect: the agent cannot fix it,
// so the run fails before any agent turn.
func TestInvalidBaseFailsBeforeTheJudgeRuns(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.baseDiagnostics = []*contracts.MemoryDiagnostic{{Rule: "detector.test", Path: ".sdo/diagnostics/detectors/health/traffic-web", Message: "stale package"}}
	_, err := h.coordinator.Run(context.Background(), config())
	var stop *lifecycle.BaseInvalidError
	if !errors.As(err, &stop) {
		t.Fatalf("error = %v, want BaseInvalidError", err)
	}
	if h.workers.judgeRuns != 0 || fake.stages != 0 {
		t.Errorf("judge runs=%d stages=%d, want none", h.workers.judgeRuns, fake.stages)
	}
	var opErr *lifecycle.OperabilityError
	if !errors.As(err, &opErr) || opErr.Decision.Owner != operability.OwnerOperator || opErr.Decision.Checks[0].Cause != operability.CauseBaseInvalid {
		t.Fatalf("decision = %+v", opErr)
	}
	if !strings.Contains(err.Error(), "detector.test") {
		t.Errorf("error does not name the rule: %v", err)
	}
}

// The deployer correcting the same feedback twice is not converging either.
func TestDeployerRepeatingTheSameCorrectionEndsAsNotConverging(t *testing.T) {
	h, fake := gateHarness(t, clear)
	same := "patch.uncaptured: 1 app file(s) differ: app.go"
	fake.scratch = []string{same, same, same}
	_, err := h.coordinator.Run(context.Background(), config())
	var stop *lifecycle.AgentNotConvergingError
	if !errors.As(err, &stop) || stop.Role != operability.OwnerDeployer {
		t.Fatalf("error = %v, want deployer AgentNotConvergingError", err)
	}
	if h.workers.deployerAttempts() != 2 {
		t.Errorf("deployer attempts = %d, want 2", h.workers.deployerAttempts())
	}
}
