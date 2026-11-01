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

func TestDevelopmentTimeScaleValidation(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    float64
		invalid bool
	}{
		{"", 1, false}, {"0.1", .1, false}, {"2", 2, false},
		{"0", 0, true}, {"-1", 0, true}, {"NaN", 0, true}, {"Inf", 0, true}, {"bad", 0, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := parseDevelopmentTimeScale(tc.value)
			if (err != nil) != tc.invalid || (!tc.invalid && got != tc.want) {
				t.Fatalf("scale %q: %v, %v", tc.value, got, err)
			}
		})
	}
}
