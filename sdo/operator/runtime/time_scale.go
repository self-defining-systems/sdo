package runtime

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

func developmentTimeScale() (float64, error) {
	raw := os.Getenv("SDO_TIME_SCALE")
	if raw == "" {
		return 1, nil
	}
	scale, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(scale) || math.IsInf(scale, 0) || scale <= 0 {
		return 0, fmt.Errorf("SDO_TIME_SCALE must be a positive finite number, got %q", raw)
	}
	return scale, nil
}

func scaledDuration(value time.Duration, scale float64) time.Duration {
	if value <= 0 || scale == 1 {
		return value
	}
	result := time.Duration(float64(value) * scale)
	if result < time.Millisecond {
		return time.Millisecond
	}
	return result
}

type timedDetector struct {
	sdk.Detector
	spec sdk.DetectorSpec
}

func (d timedDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d timedDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.Detector.Detect(ctx, view)
}

func (d timedDetector) Confirmed(view sdk.DetectionContext) bool {
	return detectorConfirmed(d.Detector, view)
}

func (d timedDetector) ConfirmationReason(view sdk.DetectionContext) string {
	if detectorConfirmed(d.Detector, view) {
		return ""
	}
	return detectorConfirmationReason(d.Detector, view)
}

func (d timedDetector) EvaluationWindow(view sdk.DetectionContext) time.Duration {
	if windowed, ok := d.Detector.(sdk.WindowedDetector); ok {
		return windowed.EvaluationWindow(view)
	}
	return 0
}

func (d timedDetector) TrafficWorkloads() []string {
	if consumer, ok := d.Detector.(traffic.Consumer); ok {
		return consumer.TrafficWorkloads()
	}
	return nil
}

func scaleDetectors(detectors []sdk.Detector, scale float64) []sdk.Detector {
	if scale == 1 {
		return detectors
	}
	result := make([]sdk.Detector, len(detectors))
	for index, detector := range detectors {
		spec := detector.Spec()
		spec.Interval = scaledDuration(spec.Interval, scale)
		spec.Batching.Debounce = scaledDuration(spec.Batching.Debounce, scale)
		spec.Persistence.MinDuration = scaledDuration(spec.Persistence.MinDuration, scale)
		result[index] = timedDetector{Detector: detector, spec: spec}
	}
	return result
}
