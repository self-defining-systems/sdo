package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sdo.dev/operator/memory"
)

// capturePreApply preserves the first fresh snapshot immediately before a live
// repair. Handle serializes ad hoc acknowledgements and typed plan application.
// The protected file is the durable marker across responder/controller restarts.
// This boundary does no diagnosis, primitive probing, or settle waiting.
func (l *ResponderLoop) capturePreApply(ctx context.Context) error {
	if l.CorpusSessionRoot == "" || l.preApplyCaptured {
		return nil
	}
	started := time.Now()
	defer func() { l.timing.capture += time.Since(started) }()
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
	if err := memory.CaptureIncidentSnapshot(l.CorpusSessionRoot, l.Request.IncidentID, "open@pre-apply", snapshot, l.Request.Namespace); err != nil {
		return fmt.Errorf("capture pre-apply snapshot: %w", err)
	}
	l.preApplyCaptured = true
	return nil
}
