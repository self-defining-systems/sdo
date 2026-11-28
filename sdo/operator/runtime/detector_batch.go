package runtime

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/core"
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
func readRecoveryDetectors(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext, scope recoveryScope) map[string]recoveryRead {
	raw := evaluateDetectors(ctx, detectors, snapshot)
	reads := make(map[string]recoveryRead, len(detectors))
	for _, detector := range detectors {
		id := detector.Spec().ID
		findings, status, err := validateDetectorObservation(detector, snapshot, raw[id].findings, raw[id].err, scope)
		reads[id] = recoveryRead{findings, status, err}
	}
	return reads
}

// findingAdmissionError is a detector result the controller refuses: one of
// its findings fails core.ValidateFinding. The detector failed; no finding of
// that evaluation is admitted. Its text is the validation's own.
type findingAdmissionError struct{ err error }

func (e findingAdmissionError) Error() string { return e.err.Error() }
func (e findingAdmissionError) Unwrap() error { return e.err }

// admitFindings is the controller's admission of one evaluation's findings
// (validateDetectorObservation): it fills the detector ID, the declared
// playbooks and the fingerprint, then validates each finding.
func admitFindings(spec sdk.DetectorSpec, findings []sdk.Finding) ([]sdk.Finding, error) {
	findings = append([]sdk.Finding(nil), findings...)
	for i := range findings {
		f := &findings[i]
		if f.DetectorID == "" {
			f.DetectorID = spec.ID
		}
		if len(f.Playbooks) == 0 {
			f.Playbooks = append([]string(nil), spec.Playbooks...)
		}
		f.Fingerprint = FindingFingerprint(*f)
		if err := core.ValidateFinding(spec, *f); err != nil {
			return nil, findingAdmissionError{err}
		}
	}
	return findings, nil
}

// evaluateAdmittedDetectors is evaluateDetectors with the controller's
// admission applied to each result. Every responder-loop and stage read uses
// it, so the stage and the responder's tools never see, bind to or target a
// finding the controller rejected.
func evaluateAdmittedDetectors(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext) map[string]setupDetection {
	results := evaluateDetectors(ctx, detectors, snapshot)
	for _, detector := range detectors {
		id := detector.Spec().ID
		if result, ok := results[id]; ok && result.err == nil {
			result.findings, result.err = admitFindings(detector.Spec(), result.findings)
			results[id] = result
		}
	}
	return results
}
