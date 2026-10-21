package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type timingDetector struct{ sdk.DetectorSpec }

func (d timingDetector) Spec() sdk.DetectorSpec { return d.DetectorSpec }
func (d timingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func TestScaleDetectorTimingKeepsIdentityAndScalesWindows(t *testing.T) {
	detector := timingDetector{sdk.DetectorSpec{ID: "health", Interval: 30 * time.Second,
		Batching:    sdk.BatchingPolicy{Debounce: 5 * time.Second},
		Persistence: sdk.PersistencePolicy{MinDuration: 10 * time.Second}}}
	scaled := scaleDetectors([]sdk.Detector{detector}, 0.1)[0].Spec()
	if scaled.ID != "health" || scaled.Interval != 3*time.Second || scaled.Batching.Debounce != 500*time.Millisecond || scaled.Persistence.MinDuration != time.Second {
		t.Fatalf("scaled detector: %+v", scaled)
	}
	if original := detector.Spec(); original.Interval != 30*time.Second {
		t.Fatalf("original detector changed: %+v", original)
	}
}
