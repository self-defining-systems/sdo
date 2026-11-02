package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

func TestHealthAdoptionDiscardsQueuedObsoleteFindings(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(0, 0).UTC()
	finding := stateFinding("health")
	finding.Severity = sdk.SeverityCritical
	obsolete := controllerDetector("health", time.Minute, finding)
	obsolete.spec.Class, obsolete.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	obsolete.spec.OriginatingCommit = "old-judge"
	obsolete.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	obsolete.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: time.Minute}
	old, err := NewController(testControllerConfig(), []sdk.Detector{obsolete}, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, old)
	if len(state.GetPendingBatch().GetFindings()) != 1 {
		t.Fatalf("fixture has no queued obsolete finding: %v", state)
	}
	refreshed := controllerDetector("health", time.Minute, sdk.Finding{})
	refreshed.spec = obsolete.spec
	adopted, err := NewController(testControllerConfig(), []sdk.Detector{refreshed}, staticProvider{}, &recordingDispatcher{}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := adopted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	adopted.discardRestoredHealthObservations(now.Add(time.Second))
	if err := adopted.Step(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if adopted.incidentOpen || len(adopted.batcher.Snapshot().Findings) > 0 || adopted.tracker.HasActiveDetector("health") {
		t.Fatal("adoption dispatched an obsolete alarm")
	}
	if refreshed.calls != 1 {
		t.Fatal("adopted health policy was not evaluated immediately")
	}
}
