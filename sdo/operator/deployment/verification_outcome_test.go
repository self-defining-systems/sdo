package deployment

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
)

func TestDecodeHealthVerdictKeepsPendingDetectorAndReason(t *testing.T) {
	stdout := []byte(`{"findings":[],"status":"pending","detector_states":[{"DetectorID":"traffic-window","Status":"pending","Reason":"observation window incomplete"},{"DetectorID":"ok","Status":"clear","Reason":"seen"}]}`)
	evaluation, err := decodeHealthVerdict(stdout, nil)
	if err != nil {
		t.Fatal(err)
	}
	verdict := verdictOf(evaluation)
	if verdict.Outcome != lifecycle.VerificationPending || len(verdict.Detectors) != 1 || verdict.Detectors[0].DetectorID != "traffic-window" || verdict.Detectors[0].Reason != "observation window incomplete" {
		t.Fatalf("verdict = %+v", verdict)
	}
}

func TestDecodeHealthVerdictKeepsActiveFindingDetail(t *testing.T) {
	stdout := []byte(`{"findings":[{"detector_id":"frontend-available","rule_id":"r","status":"active","summary":"frontend has 0 ready replicas","evidence":"ready=0/1"}],"status":"firing"}`)
	evaluation, err := decodeHealthVerdict(stdout, nil)
	if err != nil {
		t.Fatal(err)
	}
	verdict := verdictOf(evaluation)
	feedback := verdict.Feedback()
	if verdict.Outcome != lifecycle.VerificationSourceFault || !strings.Contains(feedback, "frontend-available") || !strings.Contains(feedback, "0 ready replicas") || !strings.Contains(feedback, "ready=0/1") {
		t.Fatalf("verdict = %+v feedback = %q", verdict, feedback)
	}
}

func TestVerdictOfClassifiesEveryProbeStatus(t *testing.T) {
	for status, want := range map[string]lifecycle.VerificationOutcome{
		"clear": lifecycle.VerificationClear, "pending": lifecycle.VerificationPending, "unavailable": lifecycle.VerificationPending,
		"firing": lifecycle.VerificationSourceFault, "active": lifecycle.VerificationSourceFault, "error": lifecycle.VerificationSourceFault,
	} {
		if got := verdictOf(HealthEvaluation{Status: status}).Outcome; got != want {
			t.Errorf("status %q outcome = %q, want %q", status, got, want)
		}
	}
}

const buildkitStartupLog = "could not connect to unix:///run/user/1000/buildkit/buildkitd.sock after 10 trials\n========== log ==========\n"

func TestBuildKitStartupFailureIsInfrastructureNotPlan(t *testing.T) {
	failure := ClassifyBuildLog("reg/app:1", "job", buildkitStartupLog)
	if failure.Cause != BuildFailureInfrastructure || !failure.InfrastructureOutcome() {
		t.Fatalf("cause = %q", failure.Cause)
	}
	real := ClassifyBuildLog("reg/app:1", "job", compileLog)
	if real.InfrastructureOutcome() {
		t.Fatal("a compile error is a source fault")
	}
}

func TestInfrastructureOutcomeCoversExistingClassifiers(t *testing.T) {
	type marker interface{ InfrastructureOutcome() bool }
	cases := map[string]struct {
		err  error
		want bool
	}{
		"build timeout":       {&BuildTimeoutError{}, true},
		"progressing rollout": {&RolloutTimeoutError{}, true},
		"stalled rollout":     {&RolloutTimeoutError{Stalled: true}, false},
		"registry transient":  {&imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: errors.New("rate limited")}, true},
		"registry missing":    {&imagesrc.ResolveError{Kind: imagesrc.ResolveMissing, Err: errors.New("no tag")}, false},
		"pin rejection":       {tagImageRejection("m.yaml", "img:1", &imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: errors.New("429")}), true},
		"pin missing":         {tagImageRejection("m.yaml", "img:1", &imagesrc.ResolveError{Kind: imagesrc.ResolveMissing, Err: errors.New("404")}), false},
	}
	for name, tc := range cases {
		var found marker
		got := errors.As(fmt.Errorf("deploy: %w", tc.err), &found) && found.InfrastructureOutcome()
		if got != tc.want {
			t.Errorf("%s: infrastructure = %v, want %v", name, got, tc.want)
		}
	}
}
