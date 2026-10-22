package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func viewHealthDetector(id string, samples ...sdk.Finding) *sequenceDetector {
	detector := controllerDetector(id, time.Second, samples...)
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	for index := range detector.samples {
		for inner := range detector.samples[index] {
			detector.samples[index][inner].Severity = sdk.SeverityCritical
		}
	}
	return detector
}

func TestControllerPublishesTheOpenIncidentsLiveView(t *testing.T) {
	health := viewHealthDetector(
		"health", stateFinding("geo"), stateFinding("geo"), stateFinding("geo"), sdk.Finding{}, sdk.Finding{},
	)
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{health},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	first := StateChange{Kind: "ConfigMap", Name: "mongo-geo-script", Change: StateChangeRemoved}
	baseline := &recordingBaseline{changes: &StateChanges{
		BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(1, 0).UTC(), Changes: []StateChange{first},
	}}
	controller.Baseline = baseline
	if state := exportState(t, controller); state.IncidentView != nil {
		t.Fatalf("no incident, no view: %#v", state.IncidentView)
	}

	for step := 0; step < 2; step++ {
		if err := controller.Step(context.Background(), time.Unix(int64(step), 0), nil); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)

	// A second fault lands after the request was taken.
	second := StateChange{Kind: "ConfigMap", Name: "mongo-rate-script", Change: StateChangeRemoved}
	baseline.changes = &StateChanges{
		BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(2, 0).UTC(), Changes: []StateChange{first, second},
	}
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	view := exportState(t, controller).IncidentView
	if view == nil || view.IncidentId != request.IncidentID || !controllerTime(view.ObservedAt).Equal(time.Unix(2, 0).UTC()) {
		t.Fatalf("the open incident must publish a live view: %#v", view)
	}
	if len(view.BlockingDetectors) != 1 || view.BlockingDetectors[0] != "health" {
		t.Fatalf("an active health detector blocks closure: %#v", view.BlockingDetectors)
	}
	if len(view.BlockingFindings) != 1 || view.BlockingFindings[0].PrimaryResource.Name != "geo" {
		t.Fatalf("the view names the blocking findings: %#v", view.BlockingFindings)
	}
	if view.StateChanges == nil || len(view.StateChanges.Changes) != 2 {
		t.Fatalf("the view carries the current diff, including changes after dispatch: %#v", view.StateChanges)
	}
	if len(request.StateChanges.Changes) != 1 {
		t.Fatalf("the dispatched request keeps its own snapshot: %#v", request.StateChanges)
	}
	if err := validateRuntimeState(exportState(t, controller)); err != nil {
		t.Fatalf("a state with a view must validate: %v", err)
	}

	// Two clear evaluations: nothing blocks any more, the view says so.
	for step := 3; step < 5; step++ {
		if err := controller.Step(context.Background(), time.Unix(int64(step), 0), nil); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	view = exportState(t, controller).IncidentView
	if view == nil || len(view.BlockingDetectors) != 0 || len(view.BlockingFindings) != 0 {
		t.Fatalf("cleared health detectors no longer block: %#v", view)
	}
	if err := wire.Validate(view); err != nil {
		t.Fatalf("project incident view: %v", err)
	}
	close(dispatcher.release)
}

func TestRestoredStateIgnoresAStaleIncidentView(t *testing.T) {
	controller, err := NewController(testControllerConfig(), []sdk.Detector{viewHealthDetector("health")},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, &recordingDispatcher{}, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	state := exportState(t, controller)
	state.IncidentView = &contracts.IncidentView{IncidentId: "old", ObservedAt: contractTimestamp(time.Unix(1, 0)), BlockingDetectors: []string{"health"}}
	if err := controller.RestoreState(state); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if view := exportState(t, controller).IncidentView; view != nil {
		t.Fatalf("a restored controller republishes its view only after it evaluates again: %#v", view)
	}
}
