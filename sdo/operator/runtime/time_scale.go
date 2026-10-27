package runtime

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"sdo.dev/operator/sdk"
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

// Unwrap exposes the wrapped detector's capabilities; only Spec is scaled.
func (d timedDetector) Unwrap() sdk.Detector { return d.Detector }

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
