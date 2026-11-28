package lifecycle_test

import (
	"context"
	"testing"

	"sdo.dev/operator/lifecycle"
)

// A judge failure starts attempt 2 over the same accepted source. The
// architecture is accepted for that source already, and the head has moved
// past it, so attempt 2 must resume the accepted architecture rather than
// propose another one against the moved head.
func TestSecondAttemptResumesTheAcceptedArchitecture(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.workers.judgeFailures = 1
	architectures := &countingArtifacts{}
	h.coordinator.Bootstrap = architectures

	cfg := config()
	result, err := h.coordinator.Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 2 || h.workers.judgeRuns != 2 {
		t.Fatalf("attempts=%d judge runs=%d, want the judge to fail once and succeed on attempt 2", result.Attempts, h.workers.judgeRuns)
	}
	if architectures.architectures != 1 {
		t.Fatalf("architecture written %d times, want once for the one accepted source", architectures.architectures)
	}
	if h.readiness.calls != 1 || h.workers.deployerAttempts() != 0 {
		t.Fatalf("the source must deploy once, got %d calls and %d deployer tasks", h.readiness.calls, h.workers.deployerAttempts())
	}
}
