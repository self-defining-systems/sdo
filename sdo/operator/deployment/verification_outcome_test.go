package deployment

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
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

// Every infrastructure kind a trusted step can produce must be recognised by
// the classifier the coordinator really uses (lifecycle.IsInfrastructureOutcome),
// directly, wrapped by this package, and wrapped by the catalog's attribution.
func TestEveryInfrastructureKindIsRecognisedByLifecycle(t *testing.T) {
	resolve := func(kind imagesrc.ResolveErrorKind) error {
		return &imagesrc.ResolveError{Kind: kind, Err: errors.New("registry said no")}
	}
	cases := map[string]struct {
		err  error
		want bool
	}{
		"build timeout":            {&BuildTimeoutError{}, true},
		"image timeout per image":  {&imagesrc.TimeoutError{Image: "r/a:1"}, true},
		"image timeout budget":     {&imagesrc.TimeoutError{Image: "r/a:1", Budget: true}, true},
		"progressing rollout":      {&RolloutTimeoutError{}, true},
		"stalled rollout":          {&RolloutTimeoutError{Stalled: true}, false},
		"registry transient":       {resolve(imagesrc.ResolveTransient), true},
		"registry missing":         {resolve(imagesrc.ResolveMissing), false},
		"registry unauthorized":    {resolve(imagesrc.ResolveUnauthorized), false},
		"registry refused":         {resolve(imagesrc.ResolveRefused), false},
		"build daemon unavailable": {&buildkit.BuildFailureError{Cause: buildkit.BuildFailureInfrastructure}, true},
		"build compile error":      {&buildkit.BuildFailureError{Cause: buildkit.BuildFailureCompile}, false},
		"build network error":      {&buildkit.BuildFailureError{Cause: buildkit.BuildFailureNetwork}, false},
		"pin transient":            {tagImageRejection("m.yaml", "img:1", resolve(imagesrc.ResolveTransient)), true},
		"pin missing":              {tagImageRejection("m.yaml", "img:1", resolve(imagesrc.ResolveMissing)), false},
		"plain error":              {errors.New("boom"), false},
	}
	for name, tc := range cases {
		attributed := &imagesrc.Error{Image: "r/a:1", SourceID: "s", Err: tc.err}
		for form, err := range map[string]error{"bare": tc.err, "wrapped": fmt.Errorf("deploy: %w", tc.err), "attributed": fmt.Errorf("deploy: %w", attributed)} {
			if got := lifecycle.IsInfrastructureOutcome(err); got != tc.want {
				t.Errorf("%s (%s): infrastructure = %v, want %v", name, form, got, tc.want)
			}
		}
	}
}
