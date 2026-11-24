package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const preApplyCaptureGap = "pre_apply_capture_gap"

// capturePreApply preserves the first fresh snapshot immediately before a live
// repair. Handle serializes ad hoc acknowledgements and typed plan application.
// The protected file is the durable marker across responder/controller restarts.
// This boundary does no diagnosis, primitive probing, or settle waiting.
func (l *ResponderLoop) capturePreApply(ctx context.Context, refs ...sdk.ObjectRef) (captureErr error) {
	if l.CorpusSessionRoot == "" {
		return nil
	}
	if l.preApplyCaptured {
		// Later plans can name targets absent from the first capture.
		l.recordSourceComparisons(ctx, "open@pre-apply", refs...)
		return nil
	}
	// Never fill a missed first boundary with a later, possibly repaired view.
	// FailureSignals are already incident/revision-bound and restored on restart.
	if slices.Contains(l.failureSignals, preApplyCaptureGap) {
		return errors.New("first-mutation snapshot unavailable after earlier capture failure")
	}
	defer func() {
		if captureErr != nil {
			l.addFailureSignal(preApplyCaptureGap)
			slog.Warn("pre-apply evidence gap; proceeding with repair", "incident_id", l.Request.IncidentID, "error", captureErr)
			if err := l.persistToolState(ctx); err != nil {
				slog.Warn("persist pre-apply evidence gap", "incident_id", l.Request.IncidentID, "error", err)
			}
		}
	}()
	started := lifecycleio.Now()
	defer func() { l.timing.capture += lifecycleio.Since(started) }()
	dir, err := memory.IncidentCorpusDir(l.CorpusSessionRoot, l.Request.IncidentID)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, "open@pre-apply.json")); err == nil {
		l.preApplyCaptured = true
		l.recordSourceComparisons(ctx, "open@pre-apply", refs...)
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect pre-apply capture: %w", err)
	}
	if l.Provider == nil {
		return errors.New("pre-apply snapshot provider is unavailable")
	}
	endSnapshot := l.phase("preapply_snapshot")
	snapshot, err := l.Provider.Snapshot(ctx)
	endSnapshot()
	if err != nil {
		return fmt.Errorf("pre-apply snapshot: %w", err)
	}
	endCapture := l.phase("preapply_evidence")
	err = memory.CapturePreApplySnapshot(l.CorpusSessionRoot, l.Request.IncidentID, snapshot, l.Request.Namespace, func() []sdktest.PrimitiveCall {
		if l.ReadView == nil {
			return nil
		}
		budget, cancel := context.WithTimeout(ctx, min(l.deadline(), sourceComparisonCaptureBudget))
		defer cancel()
		return boundaryObservations(budget, snapshot, l.ReadView(snapshot), refs...)
	}, append(findingCaptureRefs(l.Request.Findings), refs...)...)
	endCapture()
	l.preApplyCaptured = err == nil
	return err
}
