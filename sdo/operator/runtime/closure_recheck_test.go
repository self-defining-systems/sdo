package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// windowDetector is a health detector whose observation window the test fills
// or reopens: unconfirmed means "not yet", active means the repair regressed.
type windowDetector struct {
	mu        sync.Mutex
	confirmed bool
	active    bool
	err       error
}

func (d *windowDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "health", Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}}
}

func (d *windowDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	if d.active {
		return []sdk.Finding{stateFinding("health")}, nil
	}
	return nil, nil
}

func (d *windowDetector) Confirmed(sdk.DetectionContext) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.confirmed
}

func (d *windowDetector) set(confirmed, active bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.confirmed, d.active = confirmed, active
}

type recordingClosureBroker struct {
	rejectingIncidentBroker
	closures []IncidentClosure
}

func (b *recordingClosureBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	b.mu.Lock()
	b.closures = append(b.closures, *cloneIncidentClosure(&closure))
	b.mu.Unlock()
	return b.rejectingIncidentBroker.ProcessClosure(ctx, closure)
}

// The recheck bound for windowDetector: clearing horizon (1×10s) plus two cadences.
const windowRecheckBound = 30 * time.Second

func newClosureRecheckController(t *testing.T, detector *windowDetector, broker IncidentBroker, now *time.Time) *Controller {
	t.Helper()
	controller, err := NewController(testControllerConfig(),
		[]sdk.Detector{controllerDetector("fault", time.Hour, stateFinding("fault")), detector},
		staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	return controller
}

// executeRecheck runs one driver pass of the pending closure effect.
func executeRecheck(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("expected a pending closure effect")
	}
	if err := controller.ExecuteClosureEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute closure: %v", err)
	}
}

func assertStillPending(t *testing.T, controller *Controller, broker *recordingClosureBroker) {
	t.Helper()
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING || controller.closureFailure != nil || broker.processCalls() != 0 {
		t.Fatalf("incomplete recheck must wait: state=%v failure=%+v processed=%d", controller.closureState, controller.closureFailure, broker.processCalls())
	}
}

func TestClosureRecheckWaitsForIncompleteWindowThenCommitsVerifiedOutcome(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	detector := &windowDetector{}
	broker := &recordingClosureBroker{}
	controller := newClosureRecheckController(t, detector, broker, &now)

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	now = now.Add(windowRecheckBound / 2)
	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)

	detector.set(true, false)
	now = now.Add(time.Second)
	executeRecheck(t, controller)
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || len(broker.closures) != 1 {
		t.Fatalf("complete window must commit: state=%v closures=%d", controller.closureState, len(broker.closures))
	}
	if got := broker.closures[0]; len(got.FinalDetectorStates) != 1 || got.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatalf("confirmed closure must stay verified: %+v", got)
	}
}

func TestClosureRecheckSupersedesWhenWindowReopens(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	detector := &windowDetector{}
	broker := &recordingClosureBroker{}
	controller := newClosureRecheckController(t, detector, broker, &now)
	var failures []ClosureFailure
	controller.OnClosureFailed = func(failure ClosureFailure) { failures = append(failures, failure) }

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	detector.set(false, true)
	now = now.Add(time.Second)
	executeRecheck(t, controller)
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED || len(failures) != 1 || broker.processCalls() != 0 ||
		!strings.Contains(failures[0].LastError, "health is active") || failures[0].Reason == contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_OBSERVER_DENIED {
		t.Fatalf("a reopened finding must supersede: state=%v failures=%+v processed=%d", controller.closureState, failures, broker.processCalls())
	}
}

func TestClosureRecheckIncompleteAtBoundCommitsUnconfirmed(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	detector := &windowDetector{}
	broker := &recordingClosureBroker{}
	controller := newClosureRecheckController(t, detector, broker, &now)
	var failures []ClosureFailure
	controller.OnClosureFailed = func(failure ClosureFailure) { failures = append(failures, failure) }

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	now = now.Add(windowRecheckBound)
	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	if controller.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE ||
		len(controller.pendingClosure.FinalDetectorStates) != 0 || len(controller.pendingClosure.PartialDetectorStates) != 2 ||
		!strings.Contains(controller.pendingClosure.UnconfirmedDetail, "health is unconfirmed") {
		t.Fatalf("bound expiry must mark the closure unconfirmed: %+v", controller.pendingClosure)
	}
	if controller.incidentVerificationWarning(controller.pendingClosure.Request.IncidentID) == nil {
		t.Fatal("an unconfirmed closure still reports the incident as verified to the responder")
	}

	executeRecheck(t, controller)
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || len(failures) != 0 || len(broker.closures) != 1 ||
		broker.closures[0].UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE {
		t.Fatalf("unconfirmed closure must commit its outcome: state=%v failures=%+v closures=%+v", controller.closureState, failures, broker.closures)
	}
}

func TestClosureRecheckUnavailableObserverAtBoundCommitsUnconfirmed(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	detector := &windowDetector{confirmed: true, err: errors.New("Get \"https://api/services\": dial tcp: connection refused")}
	broker := &recordingClosureBroker{}
	controller := newClosureRecheckController(t, detector, broker, &now)

	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	now = now.Add(windowRecheckBound)
	executeRecheck(t, controller)
	assertStillPending(t, controller, broker)
	if controller.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE ||
		len(controller.pendingClosure.FinalDetectorStates) != 0 || !strings.Contains(controller.pendingClosure.UnconfirmedDetail, "connection refused") {
		t.Fatalf("an unavailable observer at the bound must commit unconfirmed: %+v", controller.pendingClosure)
	}
	executeRecheck(t, controller)
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_COMMITTED || len(broker.closures) != 1 {
		t.Fatalf("unconfirmed closure must commit: state=%v closures=%d", controller.closureState, len(broker.closures))
	}
}

func TestClosureRecheckDeniedObserverFailsWithTypedReason(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	detector := &windowDetector{confirmed: true, err: apierrors.NewForbidden(schema.GroupResource{Resource: "services"}, "", errors.New("RBAC denied"))}
	broker := &recordingClosureBroker{}
	controller := newClosureRecheckController(t, detector, broker, &now)
	var failures []ClosureFailure
	controller.OnClosureFailed = func(failure ClosureFailure) { failures = append(failures, failure) }

	executeRecheck(t, controller)
	if controller.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED || len(failures) != 1 || broker.processCalls() != 0 ||
		failures[0].Reason != contracts.ClosureFailureReason_CLOSURE_FAILURE_REASON_OBSERVER_DENIED {
		t.Fatalf("a denied observer must fail with its typed reason: state=%v failures=%+v", controller.closureState, failures)
	}
}

func TestClosureRecheckPreservesNamedBlockersInOutcomeHistory(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	controller := newClosureRecheckController(t, &windowDetector{}, &recordingClosureBroker{}, &now)
	executeRecheck(t, controller)
	now = now.Add(windowRecheckBound)
	executeRecheck(t, controller)
	found := false
	for _, state := range controller.pendingClosure.PartialDetectorStates {
		if state.DetectorID == "health" && state.Status == DetectorEvaluationPending && strings.Contains(state.Reason, "observation window is incomplete") {
			found = true
		}
	}
	if !found {
		t.Fatalf("unconfirmed outcome history lost its named observation blocker: %+v", controller.pendingClosure.PartialDetectorStates)
	}
}
