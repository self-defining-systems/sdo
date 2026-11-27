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
	"sync"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const preApplyCaptureGap = "pre_apply_capture_gap"

// capturePreApply preserves the pre-write boundary immediately before a live
// repair. Handle serializes ad hoc acknowledgements and typed plan application.
// The protected file is the durable marker across responder/controller restarts.
// This boundary does no diagnosis, primitive probing, or settle waiting.
//
// reuse, when set, is the snapshot the repair was just diagnosed and planned
// on with no write since (the automatic stage pins it right before planning);
// it stands for a fresh one, so the boundary takes no second snapshot (pod
// reads, vantage probes). The object capture and every source comparison still
// read live objects, so all of them stay before the write: a repair may change
// any object, and the boundary must show it unrepaired. They run concurrently.
func (l *ResponderLoop) capturePreApply(ctx context.Context, reuse sdk.DetectionContext, refs ...sdk.ObjectRef) error {
	snapshot, err := l.capturePreApplySnapshot(ctx, reuse, refs...)
	if err == nil {
		l.recordSourceComparisons(ctx, "open@pre-apply", snapshot, refs...)
	}
	return err
}

// capturePreApplySnapshot writes the first pre-apply capture and returns the
// snapshot it read (reuse, else a fresh one); nil when it captured nothing.
// The planned objects' source comparisons on a new capture are recorded here,
// concurrently with the capture, after the boundary's own observations.
func (l *ResponderLoop) capturePreApplySnapshot(ctx context.Context, reuse sdk.DetectionContext, refs ...sdk.ObjectRef) (taken sdk.DetectionContext, captureErr error) {
	if l.CorpusSessionRoot == "" {
		return nil, nil
	}
	if l.preApplyCaptured {
		return reuse, nil
	}
	// Never fill a missed first boundary with a later, possibly repaired view.
	// FailureSignals are already incident/revision-bound and restored on restart.
	if slices.Contains(l.failureSignals, preApplyCaptureGap) {
		return nil, errors.New("first-mutation snapshot unavailable after earlier capture failure")
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
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, "open@pre-apply.json")); err == nil {
		l.preApplyCaptured = true
		return reuse, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect pre-apply capture: %w", err)
	}
	snapshot := reuse
	if snapshot == nil {
		if l.Provider == nil {
			return nil, errors.New("pre-apply snapshot provider is unavailable")
		}
		endSnapshot := l.phase("preapply_snapshot")
		snapshot, err = l.Provider.Snapshot(ctx)
		endSnapshot()
		if err != nil {
			return nil, fmt.Errorf("pre-apply snapshot: %w", err)
		}
	}
	// The capture, the boundary observations and the planned objects' source
	// comparisons are independent live reads of the same pre-write state.
	endWrite := l.phase("preapply_capture_write")
	var boundary, planned []sdktest.PrimitiveCall
	var compared []sdk.ObjectRef
	pending := l.uncompared("open@pre-apply", refs)
	var reads sync.WaitGroup
	if l.ReadView != nil {
		reads.Add(2)
		go func() {
			defer reads.Done()
			// Evidence never delays a write for longer than a diagnosis may take.
			budget, cancel := context.WithTimeout(ctx, min(l.deadline(), sourceComparisonCaptureBudget))
			defer cancel()
			boundary = boundaryObservations(budget, snapshot, l.ReadView(snapshot))
		}()
		go func() {
			defer reads.Done()
			planned, compared = sourceComparisons(l.Request.IncidentID, l.ReadView(snapshot), pending)
		}()
	}
	err = memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, "open@pre-apply", snapshot, l.Request.Namespace, append(findingCaptureRefs(l.Request.Findings), refs...)...)
	reads.Wait()
	endWrite()
	if err != nil {
		return nil, fmt.Errorf("capture pre-apply snapshot: %w", err)
	}
	l.preApplyCaptured = true
	if l.ReadView != nil {
		// The boundary's observations first: a state read keeps its first
		// observation per phase, as when they were recorded one after the other.
		if err := memory.AppendCapturedRecordings(l.CorpusSessionRoot, l.Request.IncidentID, "open@pre-apply", append(boundary, planned...)); err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "error", err)
		} else {
			l.markCompared("open@pre-apply", compared)
		}
	}
	return snapshot, nil
}
