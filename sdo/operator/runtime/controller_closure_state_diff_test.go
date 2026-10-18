package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// TestIncidentClosureCarriesTheStateDiffAsOfVerificationTime reproduces the late-component case:
// on a composite fault (K2), the readiness-probe component lands a few
// seconds after the selector component, while the incident is already open.
// The dispatched request's diff is an immutable snapshot taken at dispatch,
// so it never sees the later fault; the closure must carry a fresh diff taken
// at verification time so evidence citing the late fault can still be
// checked, instead of being contradicted only because it landed too late for
// the snapshot.
func TestIncidentClosureCarriesTheStateDiffAsOfVerificationTime(t *testing.T) {
	interval := time.Second
	cause := controllerDetector(
		"cause", interval, stateFinding("cause"), stateFinding("cause"),
		sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{},
	)
	health := controllerDetector(
		"health", interval, stateFinding("health"), stateFinding("health"), stateFinding("health"), stateFinding("health"),
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

	selectorFault := StateChange{Kind: "Service", Name: "frontend", Change: StateChangeModified}
	readinessFault := StateChange{Kind: "Deployment", Name: "frontend", Change: StateChangeModified}
	baseline := &recordingBaseline{changes: &StateChanges{
		BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(1, 0).UTC(), Changes: []StateChange{selectorFault},
	}}
	controller.Baseline = baseline

	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.StateChanges.Changes) != 1 || request.StateChanges.Changes[0].Kind != "Service" {
		t.Fatalf("the dispatch snapshot should carry only the selector fault: %+v", request.StateChanges)
	}

	// The readiness-probe fault lands about 6s after dispatch, while the
	// incident is still open; the baseline now reports both.
	baseline.changes = &StateChanges{
		BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(6, 0).UTC(),
		Changes: []StateChange{selectorFault, readinessFault},
	}

	awaitDispatchCompletionQueued(t, controller)
	for sample := 2; sample < 6; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("clearing step %d: %v", sample, err)
		}
	}

	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("verified incident did not emit closure")
	}
	if len(closure.Request.StateChanges.Changes) != 1 {
		t.Fatalf("the closure's request keeps its own dispatch-time snapshot: %+v", closure.Request.StateChanges)
	}
	if closure.FinalStateChanges == nil || len(closure.FinalStateChanges.Changes) != 2 {
		t.Fatalf("the closure must carry the diff as of verification time, including the late fault: %+v", closure.FinalStateChanges)
	}
	names := map[string]bool{}
	for _, change := range closure.FinalStateChanges.Changes {
		names[change.Kind+"/"+change.Name] = true
	}
	if !names["Service/frontend"] || !names["Deployment/frontend"] {
		t.Fatalf("the verification-time diff must name both faults: %+v", closure.FinalStateChanges.Changes)
	}
}

// TestIncidentClosureNeverInventsAStateChangeItNeverSaw guards the other side
// of that case: a closure with no baseline configured never fabricates a diff, and
// one with a baseline never reports a change that was not actually observed.
func TestIncidentClosureNeverInventsAStateChangeItNeverSaw(t *testing.T) {
	cause := controllerDetector("cause", time.Second, stateFinding("cause"), stateFinding("cause"), sdk.Finding{}, sdk.Finding{})
	health := controllerDetector("health", time.Second, stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{})
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
	// No baseline configured, as in a cluster where the state tracker never started.
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
	for sample := 2; sample < 4; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("clearing step %d: %v", sample, err)
		}
	}

	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("verified incident did not emit closure")
	}
	if closure.Request.StateChanges != nil {
		t.Fatalf("without a baseline the request must not carry a diff: %+v", closure.Request.StateChanges)
	}
	if closure.FinalStateChanges != nil {
		t.Fatalf("without a baseline the closure must not invent a diff: %+v", closure.FinalStateChanges)
	}
}
