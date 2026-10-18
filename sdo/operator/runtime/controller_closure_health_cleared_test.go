package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// TestIncidentClosureRecordsWhenHealthBeganItsFinalClearStreak covers the
// controller fact behind repair attribution: a responder's repair can
// back a root cause only if it started before health cleared. A health
// detector that flaps clear and fires again has not cleared; the closure
// records the start of the final streak of clear evaluations.
func TestIncidentClosureRecordsWhenHealthBeganItsFinalClearStreak(t *testing.T) {
	interval := time.Second
	cause := controllerDetector(
		"cause", interval, stateFinding("cause"), stateFinding("cause"),
		sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{},
	)
	// Firing at 0-1, a transient clear at 2, firing again at 3, clear from 4.
	health := controllerDetector(
		"health", interval, stateFinding("health"), stateFinding("health"), sdk.Finding{}, stateFinding("health"),
		sdk.Finding{}, sdk.Finding{},
	)
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	health.spec.OriginatingCommit = "health-objective"
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{cause, health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	for sample := 2; sample < 6; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("step %d: %v", sample, err)
		}
		if sample == 4 {
			state := controller.ExportState()
			if got := state.DetectorClearSince["health"]; !got.Equal(time.Unix(4, 0).UTC()) {
				t.Fatalf("the persisted clear streak must start at the latest clear evaluation, got %v", got)
			}
		}
	}

	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("verified incident did not emit closure")
	}
	if closure.HealthClearedAt == nil {
		t.Fatal("a verified closure must record when health cleared")
	}
	if want := time.Unix(4, 0).UTC(); !closure.HealthClearedAt.Equal(want) {
		t.Fatalf("health cleared at %v, want the start of the final clear streak %v", closure.HealthClearedAt, want)
	}
	if closure.HealthClearedAt.After(closure.VerifiedAt) {
		t.Fatalf("health cannot clear after verification: %v > %v", closure.HealthClearedAt, closure.VerifiedAt)
	}
}

// TestHealthClearStreakSurvivesARestart keeps the fact deterministic across a
// controller restart mid-incident: restoring the state must not restart the
// streak at the next evaluation, which would credit a later repair.
func TestHealthClearStreakSurvivesARestart(t *testing.T) {
	health := controllerDetector("health", time.Second, sdk.Finding{})
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	health.spec.OriginatingCommit = "health-objective"
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	state := controller.ExportState()
	state.DetectorClearSince = map[string]time.Time{"health": time.Unix(3, 0).UTC()}
	if err := controller.RestoreState(state); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := controller.ExportState().DetectorClearSince["health"]; !got.Equal(time.Unix(3, 0).UTC()) {
		t.Fatalf("restored clear streak = %v", got)
	}
}
