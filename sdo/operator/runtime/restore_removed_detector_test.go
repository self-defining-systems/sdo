package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func removedDetectorFinding(detectorID string) sdk.Finding {
	return sdk.Finding{
		DetectorID: detectorID, RuleID: "fault", Fingerprint: detectorID + "-fault", Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: "synthetic", Evidence: "synthetic sample",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: detectorID},
	}
}

// A controller restarts after a memory swap removed a detector and before the
// state was written again. Everything the state names about that detector is
// dropped; the controller starts, keeps the detectors that stayed, and is not
// held non-quiet by a finding nobody will ever clear.
func TestRestoreDropsStateOfDetectorsTheActiveMemoryLacks(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	config := testControllerConfig()
	config.FiringThreshold, config.ClearThreshold = 1, 1
	kept := controllerDetector("kept", time.Second, sdk.Finding{})
	removed := controllerDetector("removed", time.Second, removedDetectorFinding("removed"))
	before, err := NewController(config, []sdk.Detector{kept, removed}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := before.EvaluateAll(ctx, now); err != nil {
		t.Fatal(err)
	}
	// A finding batched by the removed detector and one it left clear-streak state for.
	before.batcher.Add(removedDetectorFinding("removed"))
	before.detectorClearSince = map[string]time.Time{"removed": now, "kept": now}
	before.incidentOpen = false
	state := exportState(t, before)
	if len(state.GetSchedulerDeadlines()) != 2 || len(state.GetFindingStates()) == 0 {
		t.Fatalf("fixture does not name the removed detector: %v", state)
	}

	after, err := NewController(config, []sdk.Detector{kept}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := after.RestoreState(state); err != nil {
		t.Fatalf("restoring state that names a removed detector: %v", err)
	}
	if deadlines := after.scheduler.Deadlines(); len(deadlines) != 1 || deadlines["kept"].IsZero() {
		t.Fatalf("schedule after restore: %v", deadlines)
	}
	if len(after.tracker.Snapshot()) != 0 || len(after.batcher.Snapshot().Findings) != 0 {
		t.Fatalf("state of the removed detector survived: %v %v", after.tracker.Snapshot(), after.batcher.Snapshot().Findings)
	}
	if !after.quiet() {
		t.Fatal("a finding of a removed detector holds the controller non-quiet")
	}
	// The same state must survive a second restart: a restore never leaves a
	// state that the next restore rejects.
	if err := after.RestoreState(exportState(t, after)); err != nil {
		t.Fatal(err)
	}
}

// A memory swap that removes a detector retires its findings immediately; waiting
// for a restart would leave them to hold the live controller non-quiet.
func TestReconfigureMemoryDropsFindingsOfRemovedDetectors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}
	config := testControllerConfig()
	config.FiringThreshold, config.ClearThreshold = 1, 1
	kept := controllerDetector("kept", time.Second, sdk.Finding{})
	removed := controllerDetector("removed", time.Second, removedDetectorFinding("removed"))
	c, err := NewController(config, []sdk.Detector{kept, removed}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.EvaluateAll(ctx, now); err != nil {
		t.Fatal(err)
	}
	c.batcher.Add(removedDetectorFinding("removed"))
	if err := c.ReconfigureMemory(&RemoteMemory{Detectors: []sdk.Detector{kept}}, provider, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(c.tracker.Snapshot()) != 0 || len(c.batcher.Snapshot().Findings) != 0 {
		t.Fatal("findings of the removed detector survived the swap")
	}
}

func TestSchedulerRestoreIgnoresUnknownDetectors(t *testing.T) {
	at := time.Unix(1000, 0).UTC()
	s := NewScheduler([]sdk.Detector{controllerDetector("kept", time.Second, sdk.Finding{})}, at)
	s.RestoreDeadlines(map[string]time.Time{"kept": at.Add(time.Hour), "gone": at.Add(time.Hour)})
	if got := s.Deadlines(); len(got) != 1 || !got["kept"].Equal(at.Add(time.Hour)) {
		t.Fatalf("deadlines %v", got)
	}
}
