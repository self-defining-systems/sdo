package lifecycle

import (
	"context"
	"errors"
	"fmt"

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
