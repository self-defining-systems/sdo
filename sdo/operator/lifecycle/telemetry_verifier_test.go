package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/telemetryapply"
)

type applierFunc func(string) (telemetryapply.Applied, error)

func (f applierFunc) Apply(_ context.Context, checkout string) (telemetryapply.Applied, error) {
	return f(checkout)
}

type verifierSpy struct{ calls int }

func (v *verifierSpy) VerifyCandidate(context.Context, string, string, string, string, DeploymentReceipt) (HealthVerdict, error) {
	v.calls++
	return HealthVerdict{Outcome: VerificationClear}, nil
}

func TestAppliedVerifierAppliesTheCheckoutBeforeVerifying(t *testing.T) {
	var applied string
	next := &verifierSpy{}
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(root string) (telemetryapply.Applied, error) {
		applied = root
		return telemetryapply.Applied{}, nil
	}), Next: next}
	verdict, err := v.VerifyCandidate(context.Background(), "/checkout", "c", "ns", "obj", DeploymentReceipt{})
	if err != nil || verdict.Outcome != VerificationClear || applied != "/checkout" || next.calls != 1 {
		t.Fatalf("verdict %v err %v applied %q calls %d", verdict, err, applied, next.calls)
	}
}

func TestAppliedVerifierTurnsATypedDenialIntoAPendingVerdictNamingTheFile(t *testing.T) {
	next := &verifierSpy{}
	denial := &telemetryapply.Error{Reason: telemetryapply.ReasonDenied, Backend: "stack", Path: "backends.yaml", Detail: "recipe takes no parameters"}
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(string) (telemetryapply.Applied, error) { return telemetryapply.Applied{}, denial }), Next: next}
	verdict, err := v.VerifyCandidate(context.Background(), "/checkout", "c", "ns", "obj", DeploymentReceipt{})
	if err != nil || verdict.Outcome != VerificationPending || next.calls != 0 {
		t.Fatalf("verdict %v err %v calls %d", verdict, err, next.calls)
	}
	for _, want := range []string{"denied", "stack", "backends.yaml", "no parameters"} {
		if !strings.Contains(verdict.Summary, want) {
			t.Errorf("summary %q lacks %q", verdict.Summary, want)
		}
	}
}

func TestAppliedVerifierReturnsInfrastructureFailuresAsRetryable(t *testing.T) {
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(string) (telemetryapply.Applied, error) { return telemetryapply.Applied{}, errors.New("api down") }), Next: &verifierSpy{}}
	_, err := v.VerifyCandidate(context.Background(), "/c", "c", "ns", "o", DeploymentReceipt{})
	if err == nil || !IsInfrastructureOutcome(err) {
		t.Fatalf("err = %v, want an infrastructure outcome", err)
	}
}

func TestAppliedVerifierRequiresItsCollaborators(t *testing.T) {
	if _, err := (TelemetryAppliedVerifier{}).VerifyCandidate(context.Background(), "/c", "c", "ns", "o", DeploymentReceipt{}); err == nil {
		t.Fatal("want a configuration error")
	}
}
