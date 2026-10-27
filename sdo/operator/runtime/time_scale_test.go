package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
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

// A scaled run (the scripted rehearsal uses 0.1) must wire the same
// observation sources as an unscaled one: the controller decides which
// observers to start from the capabilities its detectors declare.
func TestScaleDetectorsKeepsObservationCapabilities(t *testing.T) {
	spec := func(id string) sdk.DetectorSpec {
		return sdk.DetectorSpec{ID: id, Interval: 30 * time.Second}
	}
	for _, scale := range []float64{1, 0.1} {
		admission := scaleDetectors([]sdk.Detector{workloadhealth.NewPodAdmissionDetector(spec("pod-admission"))}, scale)
		if !sdk.ConsumesPodAdmission(admission) {
			t.Errorf("scale %v: pod admission consumer lost", scale)
		}
		reachability := scaleDetectors([]sdk.Detector{servicehealth.NewReachabilityDetector(spec("service-reachability"))}, scale)
		if !ConsumesServiceReachability(reachability) {
			t.Errorf("scale %v: service reachability consumer lost", scale)
		}
		alerts := scaleDetectors([]sdk.Detector{metrics.NewAlertsDetector(spec("alerts"), metrics.LabelMapping{})}, scale)
		if !metrics.ConsumesAlerts(alerts) {
			t.Errorf("scale %v: alert consumer lost", scale)
		}
		thresholds := scaleDetectors([]sdk.Detector{metrics.NewThresholdDetector(spec("latency"), []metrics.ThresholdRule{{
			Query: metrics.Query{ID: "p99", Expr: "up"}, Op: metrics.Greater, Threshold: 1}})}, scale)
		if queries, err := metrics.Queries(thresholds); err != nil || len(queries) != 1 {
			t.Errorf("scale %v: metric queries = %v, %v", scale, queries, err)
		}
	}
}
