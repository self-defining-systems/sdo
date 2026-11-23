package deployment

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/lifecycle"
)

// A probe that cannot compile before the deployment is known has no session: the lifecycle verifies after the
// rollout, as it always did.
func TestAVerifierWithoutACompilingProbeHasNoSession(t *testing.T) {
	v := IndependentHealthVerifier{AcceptedRoot: "/a", OperatorRoot: "/o", Readiness: &Deployer{}, Probe: noCompileProbe{}}
	if _, err := v.BeginCandidate(context.Background(), "/a", "c", "ns", "obj"); !errors.Is(err, lifecycle.ErrNoSession) {
		t.Fatalf("err = %v, want ErrNoSession", err)
	}
}

type noCompileProbe struct{}

func (noCompileProbe) Evaluate(context.Context, string, string, string, HealthScope) (HealthEvaluation, error) {
	return HealthEvaluation{Status: "clear"}, nil
}
