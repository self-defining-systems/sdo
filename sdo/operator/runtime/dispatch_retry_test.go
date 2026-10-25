package runtime

import (
	"errors"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// newDispatchRetryController builds a controller with an open incident whose
// dispatch effect is pending, without going through Step()/the scheduler, so
// backoff timing is deterministic and never races the detector's own
// schedule. The detector's hour-long interval (mirroring
// newClosureRetryController) keeps it from ever becoming due during the test.
func newDispatchRetryController(t *testing.T, policy DispatchRetryPolicy, now *time.Time) (*Controller, IncidentRequest) {
	t.Helper()
	config := testControllerConfig()
	config.DispatchRetry = policy
	controller, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		now.Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.now = func() time.Time { return *now }
	request := controller.incidentRequest(*now, []sdk.Finding{stateFinding("fault")})
	controller.mu.Lock()
	controller.incidentOpen = true
	controller.responderDone = false
	controller.currentIncidentRequest = cloneIncidentRequest(&request)
	controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	controller.incidentDetectedAt = *now
	controller.mu.Unlock()
	return controller, request
}

func TestTransientDispatchFailureRetriesWithBoundedJitteredBackoff(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	controller, request := newDispatchRetryController(t, DispatchRetryPolicy{
		InitialBackoff: time.Second, MaxBackoff: 3 * time.Second, JitterFraction: 0.5,
	}, &now)
	controller.dispatchJitter = func() float64 { return 0 }

	if _, ok := controller.PendingDispatchEffect(); !ok {
		t.Fatal("expected a pending dispatch effect before any failure")
	}
	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: request.IncidentID, err: errors.New("transient result watch failure"),
	}, now)

	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("a transient dispatch failure retried without backoff")
	}
	// backoff(1) = InitialBackoff = 1s; jittered with draw=0 and
	// JitterFraction=0.5 floors to (1-0.5)*1s = 500ms.
	firstDelay := 500 * time.Millisecond
	if wake := controller.NextWake(); !wake.Equal(now.Add(firstDelay)) {
		t.Fatalf("controller does not wake for the dispatch retry: got %s want %s", wake, now.Add(firstDelay))
	}

	now = now.Add(firstDelay - time.Millisecond)
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("dispatch retry ran before its backoff elapsed")
	}

	now = now.Add(time.Millisecond)
	retry, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("dispatch retry did not become ready once its backoff elapsed")
	}
	if retry.Request.IncidentID != request.IncidentID {
		t.Fatalf("dispatch retry changed incident id: got %q want %q", retry.Request.IncidentID, request.IncidentID)
	}

	// A second consecutive failure backs off further (still bounded by MaxBackoff).
	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: request.IncidentID, err: errors.New("transient result watch failure"),
	}, now)
	// backoff(2) = 2s, jittered floor = 1s.
	secondDelay := time.Second
	if wake := controller.NextWake(); !wake.Equal(now.Add(secondDelay)) {
		t.Fatalf("controller does not wake for the second dispatch retry: got %s want %s", wake, now.Add(secondDelay))
	}

	// A third failure never exceeds MaxBackoff.
	now = now.Add(secondDelay)
	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: request.IncidentID, err: errors.New("transient result watch failure"),
	}, now)
	// backoff(3) caps at MaxBackoff=3s, jittered floor = 1.5s.
	thirdDelay := 1500 * time.Millisecond
	if wake := controller.NextWake(); !wake.Equal(now.Add(thirdDelay)) {
		t.Fatalf("dispatch retry backoff exceeded its cap: got wake %s want %s", wake, now.Add(thirdDelay))
	}

	// A successful dispatch resets the backoff so the next incident starts clean.
	now = now.Add(thirdDelay)
	if _, ok := controller.PendingDispatchEffect(); !ok {
		t.Fatal("dispatch retry did not become ready once its backoff elapsed")
	}
	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: request.IncidentID, result: completedResult(request.IncidentID),
	}, now)
	if controller.dispatchFailureAttempts != 0 || !controller.dispatchNextRetryAt.IsZero() {
		t.Fatalf(
			"a completed dispatch did not reset retry backoff: attempts=%d nextRetryAt=%s",
			controller.dispatchFailureAttempts, controller.dispatchNextRetryAt,
		)
	}
}

// A responder Job that Kubernetes marks failed is a terminal dispatch
// failure: it must never be scheduled for another attempt, regardless
// of the dispatch retry backoff policy.
func TestTerminalDispatchFailureNeverRetriesUnderBackoffPolicy(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	controller, request := newDispatchRetryController(
		t, DispatchRetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, &now,
	)

	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: request.IncidentID, err: &ResponderJobFailedError{JobName: IncidentJobName(request.IncidentID)},
	}, now)

	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("a terminal dispatch failure was scheduled for another attempt")
	}
	if wake := controller.NextWake(); wake.Equal(now.Add(time.Millisecond)) {
		t.Fatalf("a terminal dispatch failure scheduled a backoff retry: wake=%s", wake)
	}
}

func TestDispatchRetryPolicyRejectsInvalidValues(t *testing.T) {
	for _, policy := range []DispatchRetryPolicy{
		{InitialBackoff: -time.Second},
		{InitialBackoff: time.Minute, MaxBackoff: time.Second},
		{JitterFraction: -0.1},
		{JitterFraction: 1.1},
	} {
		config := testControllerConfig()
		config.DispatchRetry = policy
		_, err := NewController(
			config,
			[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
			staticProvider{snapshot: sdktest.Snapshot{}},
			&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
			time.Unix(0, 0),
		)
		if err == nil {
			t.Fatalf("policy %#v was accepted", policy)
		}
	}
}
