package runtime

import (
	"context"
	"fmt"
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
	byID := make(map[string]setupDetection, len(detectors))
	evaluateDetectorsEach(ctx, detectors, snapshot, func(detector sdk.Detector, result setupDetection) {
		byID[detector.Spec().ID] = result
	})
	return byID
}

// evaluateDetectorsEach is evaluateDetectors that hands each result to apply,
// on the calling goroutine, as soon as that detector returns. apply never runs
// concurrently with itself, so it may touch controller state.
func evaluateDetectorsEach(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext, apply func(sdk.Detector, setupDetection)) {
	// Tool calls already own a scope across detector and program evaluation.
	// Reuse it so both phases pin the same mirror instead of capturing twice.
	if scope, _ := ctx.Value(memoryScopeKey{}).(*memoryScope); scope == nil {
		var releaseScope func()
		ctx, releaseScope = withMemoryScope(ctx)
		defer releaseScope()
	}
	type finished struct {
		index  int
		result setupDetection
	}
	done := make(chan finished, len(detectors))
	slots := make(chan struct{}, maxConcurrentDetectors)
	// Launching must not block applying: a full slot pool would otherwise hold
	// back the results of detectors that already returned.
	go func() {
		for i, detector := range detectors {
			slots <- struct{}{}
			go func() {
				defer func() { <-slots }()
				spec := detector.Spec()
				var result setupDetection
				defer func() {
					if r := recover(); r != nil {
						result = setupDetection{err: fmt.Errorf("detector %q panicked: %v", spec.ID, r)}
					}
					done <- finished{i, result}
				}()
				if preparation, ok := detector.(interface {
					prepareDetection(context.Context, sdk.DetectionContext) (sdk.Detector, func(), error)
				}); ok {
					prepared, release, err := preparation.prepareDetection(ctx, snapshot)
					if err != nil {
						result = setupDetection{err: err}
						return
					}
					defer release()
					detector = prepared
				}
				bounded, cancel := context.WithTimeout(ctx, detectorDeadline(spec))
				defer cancel()
				findings, err := sdk.DetectObserved(bounded, detector, snapshot)
				result = setupDetection{findings, err}
			}()
		}
	}()
	for range detectors {
		f := <-done
		apply(detectors[f.index], f.result)
	}
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
