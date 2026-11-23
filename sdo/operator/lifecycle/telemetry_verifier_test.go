package lifecycle

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/rendered"
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

type innerSession struct{ log *[]string }

func (s innerSession) Applied(context.Context, rendered.ReceiptPins) error {
	*s.log = append(*s.log, "inner applied")
	return nil
}
func (s innerSession) Verdict(context.Context, DeploymentReceipt, time.Time) (HealthVerdict, error) {
	*s.log = append(*s.log, "inner verdict")
	return HealthVerdict{Outcome: VerificationClear}, nil
}
func (s innerSession) Close() { *s.log = append(*s.log, "inner closed") }

type sessionSpy struct {
	verifierSpy
	log *[]string
}

func (v *sessionSpy) BeginCandidate(context.Context, string, string, string, string) (CandidateSession, error) {
	return innerSession{log: v.log}, nil
}

// A session applies the candidate's backends when the deployment is applied, before the inner session starts
// observing, and an unapplied backend is the pending verdict VerifyCandidate gives.
func TestTelemetrySessionAppliesBackendsBeforeTheInnerSessionObserves(t *testing.T) {
	var log []string
	next := &sessionSpy{log: &log}
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(root string) (telemetryapply.Applied, error) {
		log = append(log, "apply "+root)
		return telemetryapply.Applied{}, nil
	}), Next: next}
	session, err := v.BeginCandidate(context.Background(), "/checkout", "c", "ns", "obj")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Applied(context.Background(), rendered.ReceiptPins{}); err != nil {
		t.Fatal(err)
	}
	verdict, err := session.Verdict(context.Background(), DeploymentReceipt{}, time.Unix(1, 0))
	session.Close()
	want := []string{"apply /checkout", "inner applied", "inner verdict", "inner closed"}
	if err != nil || verdict.Outcome != VerificationClear || !slices.Equal(log, want) {
		t.Fatalf("verdict %v err %v log %q, want %q", verdict, err, log, want)
	}
}

func TestTelemetrySessionTurnsATypedDenialIntoAPendingVerdict(t *testing.T) {
	var log []string
	denial := &telemetryapply.Error{Reason: telemetryapply.ReasonDenied, Backend: "stack", Path: "backends.yaml", Detail: "no parameters"}
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(string) (telemetryapply.Applied, error) { return telemetryapply.Applied{}, denial }), Next: &sessionSpy{log: &log}}
	session, _ := v.BeginCandidate(context.Background(), "/checkout", "c", "ns", "obj")
	_ = session.Applied(context.Background(), rendered.ReceiptPins{})
	verdict, err := session.Verdict(context.Background(), DeploymentReceipt{}, time.Unix(1, 0))
	if err != nil || verdict.Outcome != VerificationPending || !strings.Contains(verdict.Summary, "backends.yaml") || slices.Contains(log, "inner applied") {
		t.Fatalf("verdict %v err %v log %q", verdict, err, log)
	}
}

// A deployment that never says it applied is verified as before, backends and a cold evaluation included.
func TestTelemetrySessionWithoutAnAppliedCallVerifiesCold(t *testing.T) {
	var log []string
	next := &sessionSpy{log: &log}
	v := TelemetryAppliedVerifier{Apply: applierFunc(func(string) (telemetryapply.Applied, error) { return telemetryapply.Applied{}, nil }), Next: next}
	session, _ := v.BeginCandidate(context.Background(), "/checkout", "c", "ns", "obj")
	verdict, err := session.Verdict(context.Background(), DeploymentReceipt{}, time.Unix(1, 0))
	if err != nil || verdict.Outcome != VerificationClear || next.calls != 1 {
		t.Fatalf("verdict %v err %v cold verifications %d", verdict, err, next.calls)
	}
}
