package runtime

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/sdk"
)

// unreadableDetector cannot read its input in this environment, as resource
// usage on a node whose kubelet exposes no CFS counters.
type unreadableDetector struct {
	spec sdk.DetectorSpec
	err  string
}

func (d unreadableDetector) Spec() sdk.DetectorSpec { return d.spec }
func (d unreadableDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, errors.New(d.err)
}

func unreadable(id string) unreadableDetector {
	return unreadableDetector{spec: classedDetector(id, sdk.DetectorClassHealth, false).spec, err: "CFS counters unavailable"}
}

// A detector that is not a user test and cannot be read in this environment
// is a coverage gap: the verdict records it with its reason and still judges
// the rest, so a healthy head is accepted and a broken one is still firing.
func TestEvaluateOnceRecordsAnUnreadableNonUserTestWithoutHolding(t *testing.T) {
	healthy := classedDetector("workload-health", sdk.DetectorClassHealth, false)
	got, err := evaluateOnceClassed(t, []sdk.Detector{healthy, unreadable("resource-saturation"), unreadable("cpu-throttling"), unreadable("traffic-links")})
	if err != nil {
		t.Fatalf("evaluate-once failed on an unreadable non-user-test detector: %v", err)
	}
	if got.Status != evaluation.Clear {
		t.Fatalf("status = %q, want clear: unreadable resource usage must not hold verification", got.Status)
	}
	ids := map[string]string{}
	for _, item := range got.Unavailable {
		ids[item.DetectorID] = item.Reason
	}
	if ids["resource-saturation"] == "" || ids["cpu-throttling"] == "" || ids["traffic-links"] == "" {
		t.Fatalf("unavailable = %+v, want the unreadable detectors recorded with their reason", got.Unavailable)
	}

	broken := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	if got, err := evaluateOnceClassed(t, []sdk.Detector{broken, unreadable("resource-saturation")}); err != nil || got.Status != evaluation.Firing {
		t.Fatalf("firing detector beside a coverage gap: %+v, %v; want firing", got, err)
	}
}

func TestEvaluateOnceUnreadableRequestMetricsHoldsVerification(t *testing.T) {
	metrics := unreadable("request-metrics")
	metrics.err = "scrape /metrics: HTTP 503"
	got, err := evaluateOnceClassed(t, []sdk.Detector{metrics})
	if err != nil || got.Status != evaluation.Unavailable || len(got.Unavailable) != 1 || got.Unavailable[0].Reason != metrics.err {
		t.Fatalf("request metrics verdict = %+v, %v; want unavailable with scrape error", got, err)
	}
}

func TestEvaluateOnceFiringClueDoesNotHoldVerification(t *testing.T) {
	link := classedDetector("traffic-links", sdk.DetectorClassHealth, true)
	got, err := evaluateOnceClassed(t, []sdk.Detector{link})
	if err != nil || got.Status != evaluation.Clear || len(got.Findings) != 1 || len(got.ClueFindings) != 1 || got.ClueFindings[0] != 0 {
		t.Fatalf("link verdict = %+v, %v; want clear with the finding still visible", got, err)
	}
}
