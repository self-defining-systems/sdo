package runtime

import (
	"errors"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Observers judge an undispatchable responder by the controller's own count of
// consecutive failed dispatch attempts, never by sampling the sticky dispatch
// error: the count survives export and restore and clears when a dispatch ends.
func TestDispatchFailureAttemptsArePublishedAndClearedOnCompletion(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	request := IncidentRequest{IncidentID: "synthetic", ResponseDeadline: now.Add(time.Hour)}
	c.incidentOpen = true
	c.currentIncidentRequest = &request
	for attempt := 1; attempt <= 3; attempt++ {
		c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
		c.handleDispatchCompletion(dispatchCompletion{incidentID: "synthetic", err: errors.New("rate_limited: pool unavailable")}, now)
		stored, err := c.ExportState()
		if err != nil {
			t.Fatal(err)
		}
		if got := stored.GetDispatchFailureAttempts(); got != int32(attempt) {
			t.Fatalf("attempts after failure %d = %d", attempt, got)
		}
		if stored.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_PENDING {
			t.Fatalf("state = %s; want PENDING", stored.GetDispatchState())
		}
		restored, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
		if err != nil {
			t.Fatal(err)
		}
		// Restore round-trips the count; the synthetic request is not a valid persisted one.
		stored.IncidentRequest, stored.IncidentOpen, stored.DispatchState = nil, false, contracts.DispatchState_DISPATCH_STATE_UNSPECIFIED
		if err := restored.RestoreState(stored); err != nil {
			t.Fatal(err)
		}
		if restored.dispatchFailureAttempts != attempt {
			t.Fatalf("restored attempts = %d; want %d", restored.dispatchFailureAttempts, attempt)
		}
	}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.handleDispatchCompletion(dispatchCompletion{incidentID: "synthetic", result: IncidentResult{IncidentID: "synthetic", Status: IncidentCompleted}}, now)
	stored, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if stored.GetDispatchFailureAttempts() != 0 {
		t.Fatalf("attempts after completion = %d; want 0", stored.GetDispatchFailureAttempts())
	}
}
