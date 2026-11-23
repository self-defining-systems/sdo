package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/telemetryapply"
)

// TelemetryAppliedVerifier verifies a candidate only after the backends its
// checkout selects are deployed. It is the apply phase for the paths that
// verify the accepted designer memory against a live deployment without
// running a designer candidate through the Coordinator gate (an upstream
// advance, a source repair): detectors bound to a backend nobody applied would
// otherwise be judged against a missing source. A typed apply failure is a
// pending verdict that names the reason and file, never a source fault; any
// other error is infrastructure and returned.
type TelemetryAppliedVerifier struct {
	Apply TelemetryApplier
	Next  CandidateVerifier
}

// VerifyCandidate applies root's backends, then verifies the candidate.
func (v TelemetryAppliedVerifier) VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source DeploymentReceipt) (HealthVerdict, error) {
	if v.Apply == nil || v.Next == nil {
		return HealthVerdict{}, errors.New("telemetry applied verifier requires an applier and a verifier")
	}
	if _, err := v.Apply.Apply(ctx, root); err != nil {
		var typed *telemetryapply.Error
		if errors.As(err, &typed) {
			return HealthVerdict{Outcome: VerificationPending, Summary: fmt.Sprintf("[%s] telemetry backend %q was not applied (correct %s): %s", typed.Reason, typed.Backend, typed.Path, typed.Detail)}, nil
		}
		if ctx.Err() != nil {
			return HealthVerdict{}, ctx.Err()
		}
		return HealthVerdict{}, &telemetryApplyInfrastructure{err: err}
	}
	return v.Next.VerifyCandidate(ctx, root, commit, namespace, objective, source)
}

// BeginCandidate lets a verifier that can overlap its work with a rollout do so: the candidate's backends are
// applied when the deployment is applied (before the session observes), and a backend that cannot be applied
// is the pending verdict VerifyCandidate gives for it.
func (v TelemetryAppliedVerifier) BeginCandidate(ctx context.Context, root, commit, namespace, objective string) (CandidateSession, error) {
	next, ok := v.Next.(SessionVerifier)
	if v.Apply == nil || !ok {
		return nil, ErrNoSession
	}
	inner, err := next.BeginCandidate(ctx, root, commit, namespace, objective)
	if err != nil {
		return nil, err
	}
	return &telemetrySession{verifier: v, root: root, commit: commit, namespace: namespace, objective: objective, inner: inner}, nil
}

type telemetrySession struct {
	verifier                     TelemetryAppliedVerifier
	root                         string
	commit, namespace, objective string
	inner                        CandidateSession
	pending                      *HealthVerdict
	applyErr                     error
	applied                      bool
}

func (s *telemetrySession) Applied(ctx context.Context, pins rendered.ReceiptPins) error {
	if s.applied {
		return nil
	}
	s.applied = true
	if _, err := s.verifier.Apply.Apply(ctx, s.root); err != nil {
		var typed *telemetryapply.Error
		if errors.As(err, &typed) {
			s.pending = &HealthVerdict{Outcome: VerificationPending, Summary: fmt.Sprintf("[%s] telemetry backend %q was not applied (correct %s): %s", typed.Reason, typed.Backend, typed.Path, typed.Detail)}
		} else {
			s.applyErr = err
		}
		return nil
	}
	return s.inner.Applied(ctx, pins)
}

func (s *telemetrySession) Verdict(ctx context.Context, source DeploymentReceipt, readyAt time.Time) (HealthVerdict, error) {
	if !s.applied {
		// The deployment never said it applied: verify as VerifyCandidate does (backends, then a cold evaluation).
		return s.verifier.VerifyCandidate(ctx, s.root, s.commit, s.namespace, s.objective, source)
	}
	if s.pending != nil {
		return *s.pending, nil
	}
	if s.applyErr != nil {
		if ctx.Err() != nil {
			return HealthVerdict{}, ctx.Err()
		}
		return HealthVerdict{}, &telemetryApplyInfrastructure{err: s.applyErr}
	}
	return s.inner.Verdict(ctx, source, readyAt)
}

func (s *telemetrySession) Close() { s.inner.Close() }
