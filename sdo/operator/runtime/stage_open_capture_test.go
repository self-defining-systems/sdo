package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"sdo.dev/operator/sdk"
)

// countingProvider counts the cluster snapshots a loop takes.
type countingProvider struct {
	SnapshotProvider
	snapshots atomic.Int32
}

func (p *countingProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	p.snapshots.Add(1)
	return p.SnapshotProvider.Snapshot(ctx)
}

func capturedOpenDetect(root string) bool {
	matches, _ := filepath.Glob(filepath.Join(root, ".sdo-runtime", "sessions", "*", "corpus", "open@detect*.json"))
	return len(matches) > 0
}

// With no learned playbook registered the automatic stage has nothing to apply
// and nothing to learn from: it diagnoses the built-in restore once and goes
// on, without retaking the open@detect capture and a second snapshot for source
// comparisons that the controller already stored when the incident opened.
func TestStageWithoutLearnedPlaybooksTakesNoOpeningCapture(t *testing.T) {
	world, stage, state := restoreStage(t, false)
	loop := stage.NewLoop(state.Request)
	root := t.TempDir()
	loop.CorpusSessionRoot = root
	provider := &countingProvider{SnapshotProvider: world}
	loop.Provider = provider
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if capturedOpenDetect(root) {
		t.Fatal("the stage retook the open@detect capture although no learned playbook was diagnosed")
	}
	if got := state.Request.PlaybookCoverage.GetMatches(); len(got) != 1 || got[0].GetPlaybookId() != SourceRestoreID {
		t.Fatalf("the built-in restore must still be offered as a match: %v", got)
	}
	if got := provider.snapshots.Load(); got != 1 {
		t.Fatalf("snapshots = %d, want the one the diagnosis needs", got)
	}
}

// A learned playbook is diagnosed against the capture, so the stage still takes it.
func TestStageWithALearnedPlaybookTakesTheOpeningCapture(t *testing.T) {
	world, stage, state := keyedStageLoop(t, nil)
	loop := stage.NewLoop(state.Request)
	root := t.TempDir()
	loop.CorpusSessionRoot = root
	loop.Provider = &countingProvider{SnapshotProvider: world}
	if _, err := stage.Run(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if !capturedOpenDetect(root) {
		entries, _ := os.ReadDir(root)
		t.Fatalf("no open@detect capture with a learned playbook registered (root has %d entries)", len(entries))
	}
}
