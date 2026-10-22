package runtime

import (
	"context"
	"errors"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// rejectingIncidentBroker rejects the first rejections closures, then commits.
type rejectingIncidentBroker struct {
	mu         sync.Mutex
	rejections int
	processed  int
}

func (b *rejectingIncidentBroker) PrepareIncident(_ context.Context, incidentID string) (IncidentWorkspace, error) {
	return IncidentWorkspace{IncidentID: incidentID, Worktree: "/worktrees/incident", BaseCommit: "base"}, nil
}

func (b *rejectingIncidentBroker) ProcessClosure(_ context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processed++
	if b.processed <= b.rejections {
		return ClosureReceipt{}, errors.New("broker exited unsuccessfully: exit status 1: source repair check failed: trailing whitespace")
	}
	return ClosureReceipt{
		IncidentID: closure.Request.IncidentID, Worktree: "/worktrees/incident", BaseCommit: "base",
		OutcomeCommit: "outcome", AckToken: "token",
	}, nil
}

func (b *rejectingIncidentBroker) AcknowledgeClosure(context.Context, ClosureReceipt) error {
	return nil
}

func (b *rejectingIncidentBroker) processCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.processed
}

func newClosureRetryController(t *testing.T, policy ClosureRetryPolicy, broker IncidentBroker, now *time.Time) *Controller {
	t.Helper()
	config := testControllerConfig()
	config.ClosureRetry = policy
	controller, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		// The detector is not due for an hour, so NextWake reflects retries.
		now.Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.now = func() time.Time { return *now }
	request := controller.incidentRequest(*now, []sdk.Finding{stateFinding("fault")})
	request.DetectorHistory = testDetectorHistory(*now)
	closure := IncidentClosure{
		Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)),
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}},
		DetectedAt:          *now, DispatchedAt: *now, ResponderCompletedAt: *now, VerifiedAt: *now,
	}
	controller.pendingClosure = &closure
	controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	return controller
}

func attemptClosure(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("expected a closure attempt to be due")
	}
	if err := controller.ExecuteClosureEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute closure: %v", err)
	}
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
}

func TestRejectedClosureRetriesWithBackoffThenFailsPermanently(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &rejectingIncidentBroker{rejections: 1000}
	controller := newClosureRetryController(t, ClosureRetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 1500 * time.Millisecond,
	}, broker, &now)
	var failures []ClosureFailure
	controller.OnClosureFailed = func(failure ClosureFailure) { failures = append(failures, failure) }

	attemptClosure(t, controller)
	if _, ok := controller.PendingClosureEffect(); ok {
		t.Fatal("a rejected closure was retried without backoff")
	}
	if wake := controller.NextWake(); !wake.Equal(now.Add(time.Second)) {
		t.Fatalf("controller does not wake for the retry: got %s want %s", wake, now.Add(time.Second))
	}
	state := exportState(t, controller)
	if state.ClosureState != contracts.ClosureState_CLOSURE_STATE_PENDING || state.ClosureFailure == nil || state.ClosureFailure.Attempts != 1 ||
		state.ClosureFailure.Permanent {
		t.Fatalf("retry state was not durable: %#v", state.ClosureFailure)
	}

	now = now.Add(time.Second)
	attemptClosure(t, controller)
	now = now.Add(time.Second)
	if _, ok := controller.PendingClosureEffect(); ok {
		t.Fatal("backoff did not grow after the second rejection")
	}
	now = now.Add(500 * time.Millisecond)
	attemptClosure(t, controller)

	now = now.Add(time.Hour)
	if _, ok := controller.PendingClosureEffect(); ok {
		t.Fatal("a permanently failed closure was retried")
	}
	if broker.processCalls() != 3 {
		t.Fatalf("broker processed %d closures, want exactly 3", broker.processCalls())
	}
	state = exportState(t, controller)
	failure := state.ClosureFailure
	if state.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED || failure == nil || !failure.Permanent || failure.Attempts != 3 {
		t.Fatalf("closure was not marked permanently failed: state=%q failure=%#v", state.ClosureState, failure)
	}
	if failure.IncidentId != state.PendingClosure.Request.IncidentId ||
		!strings.Contains(failure.LastError, "trailing whitespace") || failure.Action == "" {
		t.Fatalf("failure lacks actionable context: %#v", failure)
	}
	if state.PendingClosure == nil {
		t.Fatal("a failed closure dropped its evidence")
	}
	if len(failures) != 1 || failures[0].Attempts != 3 {
		t.Fatalf("permanent failure was not reported exactly once: %#v", failures)
	}
	if err := ClosureFailureError(controller); err == nil || !strings.Contains(err.Error(), failure.IncidentId) {
		t.Fatalf("closure failure error lacks the incident: %v", err)
	}

	restored := newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)
	if err := restored.RestoreState(state); err != nil {
		t.Fatalf("restore failed closure: %v", err)
	}
	if _, ok := restored.PendingClosureEffect(); ok {
		t.Fatal("a restart re-armed a permanently failed closure")
	}
	if exportState(t, restored).ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED {
		t.Fatal("failed closure state did not survive a restart")
	}
}

func TestClosureCommittedAfterTransientRejectionClearsFailure(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &rejectingIncidentBroker{rejections: 1}
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)

	attemptClosure(t, controller)
	now = now.Add(defaultClosureRetryInitialBackoff)
	attemptClosure(t, controller)

	state := exportState(t, controller)
	if state.ClosureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || state.ClosureFailure != nil {
		t.Fatalf("a committed closure kept its failure record: state=%q failure=%#v", state.ClosureState, state.ClosureFailure)
	}
	if err := ClosureFailureError(controller); err != nil {
		t.Fatalf("committed closure reported a failure: %v", err)
	}
}

func TestClosureRetryPolicyRejectsInvalidValues(t *testing.T) {
	for _, policy := range []ClosureRetryPolicy{
		{MaxAttempts: -1},
		{InitialBackoff: -time.Second},
		{InitialBackoff: time.Minute, MaxBackoff: time.Second},
	} {
		config := testControllerConfig()
		config.ClosureRetry = policy
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

func TestRuntimeStateRejectsFailedClosureWithoutFailureRecord(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	state := exportState(t, controller)
	state.ClosureState = contracts.ClosureState_CLOSURE_STATE_FAILED
	if err := validateRuntimeState(state); err == nil {
		t.Fatal("failed closure without a failure record was accepted")
	}
	state.ClosureFailure = &contracts.ClosureFailure{IncidentId: "other", Attempts: 1, Permanent: true}
	if err := validateRuntimeState(state); err == nil {
		t.Fatal("closure failure for another incident was accepted")
	}
}

// Maintenance pauses observation of the application, not operational
// memory. A closure rejected while paused (a reflection the validator
// refused) must still be retried, or the drain that waits for its
// acknowledgment deadlocks.
func TestAPausedControllerStillRetriesARejectedClosure(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &rejectingIncidentBroker{rejections: 1}
	controller := newClosureRetryController(t, ClosureRetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Second,
	}, broker, &now)
	attemptClosure(t, controller)

	if wake := controller.PausedWake(); !wake.Equal(now.Add(time.Second)) {
		t.Fatalf("a paused controller must wake for the closure retry: got %s", wake)
	}
	now = now.Add(time.Second)
	if err := executePausedEffects(context.Background(), controller); err != nil {
		t.Fatalf("paused effects: %v", err)
	}
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if broker.processCalls() != 2 || exportState(t, controller).ClosureState == contracts.ClosureState_CLOSURE_STATE_PENDING {
		t.Fatalf("the paused controller did not retry the closure: calls=%d state=%q",
			broker.processCalls(), exportState(t, controller).ClosureState)
	}
}

func TestPausedControllerStillWakesForAPendingClosureRetry(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &rejectingIncidentBroker{rejections: 1}
	controller := newClosureRetryController(t, ClosureRetryPolicy{
		MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}, broker, &now)

	if wake := controller.PausedWake(); !wake.IsZero() {
		t.Fatalf("nothing is waiting to retry, got a wake at %s", wake)
	}
	attemptClosure(t, controller)
	if wake := controller.PausedWake(); !wake.Equal(now.Add(time.Second)) {
		t.Fatalf("a paused controller does not wake for the closure retry: got %s want %s", wake, now.Add(time.Second))
	}

	now = now.Add(time.Second)
	attemptClosure(t, controller)
	if wake := controller.PausedWake(); !wake.IsZero() {
		t.Fatalf("a committed closure still schedules a retry at %s", wake)
	}
}

func TestFailedClosureDoesNotBlockNextIncident(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	broker := &rejectingIncidentBroker{rejections: 1000}
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 1}, broker, &now)
	attemptClosure(t, controller)
	controller.batcher.AddAt(stateFinding("next-fault"), now)
	if err := controller.dispatchReady(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() {
		t.Fatal("a permanently failed closure blocked the next incident")
	}
}

func TestClosureRetryBudgetSurvivesRestartConfigurationChange(t *testing.T) {
	now := time.Unix(1000, 0)
	broker := &rejectingIncidentBroker{rejections: 10}
	first := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 2}, broker, &now)
	attemptClosure(t, first)
	state := exportState(t, first)
	restored := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 8}, broker, &now)
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	now = now.Add(defaultClosureRetryInitialBackoff)
	attemptClosure(t, restored)
	failure, ok := restored.ClosureFailure()
	if !ok || !failure.Permanent || failure.MaxAttempts != 2 {
		t.Fatalf("restart changed incident retry budget: %+v", failure)
	}
}
