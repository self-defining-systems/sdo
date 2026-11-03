package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
)

const (
	// maxConcurrentDetectors bounds how many detectors of one batch run at once.
	maxConcurrentDetectors = 8
	// minDetectorDeadline is the least time one detector evaluation may take,
	// whatever its interval. The memory host's transport budget still ends an
	// evaluation sooner when it is shorter.
	minDetectorDeadline = 10 * time.Second
)

// detectorDeadline is how long one evaluation of a detector may run: its own
// interval, at least minDetectorDeadline. A detector that hangs is thereby
// delaying its batch by about one interval, not by the transport budget.
func detectorDeadline(spec sdk.DetectorSpec) time.Duration {
	return max(spec.Interval, minDetectorDeadline)
}

// evaluateDetectors runs a batch against one snapshot, up to
// maxConcurrentDetectors at a time, each under its own deadline, and returns
// when all are done. It decides nothing: results are keyed by detector ID and
// the caller applies them in batch order, so the controller's state changes do
// not depend on which detector finished first. A panic or a missed deadline is
// that detector's error and delays no other detector's evaluation.
func evaluateDetectors(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext) map[string]setupDetection {
	results := make([]setupDetection, len(detectors))
	slots := make(chan struct{}, maxConcurrentDetectors)
	var wg sync.WaitGroup
	for i, detector := range detectors {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			spec := detector.Spec()
			defer func() {
				if r := recover(); r != nil {
					results[i] = setupDetection{err: fmt.Errorf("detector %q panicked: %v", spec.ID, r)}
				}
			}()
			bounded, cancel := context.WithTimeout(ctx, detectorDeadline(spec))
			defer cancel()
			findings, err := sdk.DetectObserved(bounded, detector, snapshot)
			results[i] = setupDetection{findings, err}
		}()
	}
	wg.Wait()
	byID := make(map[string]setupDetection, len(detectors))
	for i, detector := range detectors {
		byID[detector.Spec().ID] = results[i]
	}
	return byID
}

// recoveryRead is one detector's validated observation for closure and recovery.
type recoveryRead struct {
	findings []sdk.Finding
	status   DetectorEvaluationStatus
	err      error
}

// readRecoveryDetectors evaluates detectors concurrently and validates each
// result, keyed by detector ID (see evaluateDetectors).
func readRecoveryDetectors(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext) map[string]recoveryRead {
	raw := evaluateDetectors(ctx, detectors, snapshot)
	reads := make(map[string]recoveryRead, len(detectors))
	for _, detector := range detectors {
		id := detector.Spec().ID
		findings, status, err := validateDetectorObservation(detector, snapshot, raw[id].findings, raw[id].err)
		reads[id] = recoveryRead{findings, status, err}
	}
	return reads
}
