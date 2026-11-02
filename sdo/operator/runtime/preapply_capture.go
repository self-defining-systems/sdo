package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"slices"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

const preApplyCaptureGap = "pre_apply_capture_gap"

// capturePreApply preserves the first fresh snapshot immediately before a live
// repair. Handle serializes ad hoc acknowledgements and typed plan application.
// The protected file is the durable marker across responder/controller restarts.
// This boundary does no diagnosis, primitive probing, or settle waiting.
func (l *ResponderLoop) capturePreApply(ctx context.Context, refs ...sdk.ObjectRef) error {
	err := l.capturePreApplySnapshot(ctx, refs...)
	if err == nil {
		l.recordSourceComparisons(ctx, "open@pre-apply", refs...)
	}
	return err
}

func (l *ResponderLoop) capturePreApplySnapshot(ctx context.Context, refs ...sdk.ObjectRef) (captureErr error) {
	if l.CorpusSessionRoot == "" || l.preApplyCaptured {
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
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect pre-apply capture: %w", err)
	}
	if l.Provider == nil {
		return errors.New("pre-apply snapshot provider is unavailable")
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("pre-apply snapshot: %w", err)
	}
	if err := memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, "open@pre-apply", snapshot, l.Request.Namespace, append(findingCaptureRefs(l.Request.Findings), refs...)...); err != nil {
		return fmt.Errorf("capture pre-apply snapshot: %w", err)
	}
	l.preApplyCaptured = true
	if l.ReadView != nil {
		// Evidence never delays a write for longer than a diagnosis may take.
		budget, cancel := context.WithTimeout(ctx, min(l.deadline(), sourceComparisonCaptureBudget))
		comparisons := boundaryObservations(budget, snapshot, l.ReadView(snapshot))
		cancel()
		if err := memory.AppendCapturedRecordings(l.CorpusSessionRoot, l.Request.IncidentID, "open@pre-apply", comparisons); err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "error", err)
		}
	}
	return nil
}
