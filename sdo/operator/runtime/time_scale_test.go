package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/workloadhealth"
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

func TestScaledDetectorsRetainObservationCapabilities(t *testing.T) {
	detectors := []sdk.Detector{
		workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "admission"}),
		servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "reachability"}),
		timingDetector{sdk.DetectorSpec{ID: "plain"}},
	}
	for _, original := range detectors {
		scaled := scaleDetectors([]sdk.Detector{original}, 0.1)
		if got, want := sdk.ConsumesPodAdmission(scaled), sdk.ConsumesPodAdmission([]sdk.Detector{original}); got != want {
			t.Errorf("%s: pod admission capability got %t, want %t", original.Spec().ID, got, want)
		}
		if got, want := ConsumesServiceReachability(scaled), ConsumesServiceReachability([]sdk.Detector{original}); got != want {
			t.Errorf("%s: reachability capability got %t, want %t", original.Spec().ID, got, want)
		}
	}
}

func TestScriptedObservationClockKeepsReachabilitySampleRequirements(t *testing.T) {
	original := ServiceReachabilityConfig{Interval: servicehealth.DefaultReachabilityInterval,
		Timeout: servicehealth.DefaultReachabilityTimeout, Failures: servicehealth.DefaultReachabilityFailures}
	scaled := scaleReachabilityTiming(original, 0.1)
	if scaled.Interval != time.Second || scaled.Timeout != original.Timeout || scaled.Failures != original.Failures {
		t.Fatalf("scripted observation timing lost sample requirements: %+v", scaled)
	}
	if production := scaleReachabilityTiming(original, 1); production.Interval != original.Interval {
		t.Fatalf("production observation clock changed: %+v", production)
	}
}
