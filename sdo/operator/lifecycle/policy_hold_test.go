package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
)

func policyViolation() *deployment.PolicyError {
	return &deployment.PolicyError{Checks: []operability.Check{{ID: "O6.allowlist", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseAllowlist, Detail: "image off the allowlist"}}}
}

func requirePolicyHold(t *testing.T, err error) {
	t.Helper()
	var refused *lifecycle.PolicyRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v, want a *PolicyRefusedError", err)
	}
	var opErr *lifecycle.OperabilityError
	if !errors.As(err, &opErr) || opErr.Decision.Kind != operability.Hold || opErr.Decision.Owner != operability.OwnerOperator {
		t.Fatalf("decision = %+v, want a hold owned by the operator", opErr)
	}
	for _, c := range opErr.Decision.Checks {
		if c.Cause != operability.CausePolicy {
			t.Fatalf("check %+v, want cause policy", c)
		}
	}
}

// A restart under a tightened policy is a typed hold with the operator as owner
// on every lifecycle path that re-verifies the accepted deployment: the run
// loop (the operate Job) and the health refresh (controller start).
func TestRestartUnderATightenedPolicyIsATypedHold(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{firing, clear}})
	h.workers.plan = rawSourcePlan(t, "k8s")
	cfg := config()
	cfg.MaxAttempts = 1
	if _, err := h.coordinator.Run(context.Background(), cfg); err == nil {
		t.Fatal("first run should exhaust its attempt on the firing verdict")
	}
	h.readiness.reverifyErr = policyViolation()
	_, err := h.coordinator.Run(context.Background(), cfg)
	requirePolicyHold(t, err)
	if h.workers.judgeRuns > 1 {
		t.Fatalf("judge ran %d times: a refused deployment must not be judged", h.workers.judgeRuns)
	}
}

func TestHealthRefreshUnderATightenedPolicyIsATypedHold(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.source = deployedSource()
	h.readiness.reverifyErr = policyViolation()
	_, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	requirePolicyHold(t, err)
	if h.workers.judgeRuns != 0 {
		t.Fatalf("judge ran %d times, want 0", h.workers.judgeRuns)
	}
}

type policyChecker struct{ err error }

func (p policyChecker) CheckAcceptedPolicy(context.Context, string, string, lifecycle.DeploymentReceipt) error {
	return p.err
}

type acceptedSource struct{ source *broker.AcceptedSource }

func (a acceptedSource) AcceptedSourceEvidence(context.Context) (broker.AcceptedSource, bool, error) {
	if a.source == nil {
		return broker.AcceptedSource{}, false, nil
	}
	return *a.source, true, nil
}

// The start-up check runs without a health refresh: it needs only the accepted
// evidence and the policy.
func TestAcceptedPolicyHold(t *testing.T) {
	ctx := context.Background()
	hold, err := lifecycle.AcceptedPolicyHold(ctx, acceptedSource{}, policyChecker{err: policyViolation()}, "commit", "ns")
	if hold != nil || err != nil {
		t.Fatalf("hold=%v err=%v, want nothing to check on a seeded install", hold, err)
	}
	hold, err = lifecycle.AcceptedPolicyHold(ctx, acceptedSource{deployedSource()}, policyChecker{}, "commit", "ns")
	if hold != nil || err != nil {
		t.Fatalf("hold=%v err=%v, want a deployment within the policy to pass", hold, err)
	}
	hold, err = lifecycle.AcceptedPolicyHold(ctx, acceptedSource{deployedSource()}, policyChecker{err: policyViolation()}, "commit", "ns")
	if err != nil || hold == nil {
		t.Fatalf("hold=%v err=%v, want a typed hold", hold, err)
	}
	requirePolicyHold(t, hold)
	boom := errors.New("git unavailable")
	if hold, err = lifecycle.AcceptedPolicyHold(ctx, acceptedSource{deployedSource()}, policyChecker{err: boom}, "commit", "ns"); hold != nil || !errors.Is(err, boom) {
		t.Fatalf("hold=%v err=%v, want the check's own failure returned", hold, err)
	}
}
