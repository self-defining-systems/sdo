package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type recordingBaseline struct {
	observations []bool
	changes      *StateChanges
	asked        []time.Time
}

func (b *recordingBaseline) Observe(_ time.Time, healthy bool) {
	b.observations = append(b.observations, healthy)
}

func (b *recordingBaseline) Changes(now time.Time) *StateChanges {
	b.asked = append(b.asked, now)
	return b.changes
}

func TestControllerAttachesStateChangesAndBaselinesOnlyHealthyEvaluations(t *testing.T) {
	interval := time.Second
	// clear, then a finding below its firing threshold, then firing.
	detector := controllerDetector("health", interval, sdk.Finding{}, stateFinding("selector"), stateFinding("selector"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	baseline := &recordingBaseline{changes: &StateChanges{
		BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(2, 0).UTC(),
		Changes: []StateChange{{Kind: "Service", Name: "frontend", Change: StateChangeModified,
			Fields: []StateFieldChange{{Field: "selector", Before: "app=frontend", After: "app=frontend,x=y"}}}},
	}}
	controller.Baseline = baseline

	start := time.Unix(0, 0)
	for step := 0; step < 3; step++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(step)*interval), nil); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)

	if len(baseline.observations) != 3 || !baseline.observations[0] || baseline.observations[1] || baseline.observations[2] {
		t.Fatalf("only the clear evaluation is healthy; a pending finding is not: %v", baseline.observations)
	}
	if request.StateChanges == nil || request.StateChanges.Changes[0].Name != "frontend" ||
		len(baseline.asked) == 0 || !baseline.asked[0].Equal(start.Add(2*interval)) {
		t.Fatalf("the dispatched request must carry the state diff taken when the incident opened: %+v", request.StateChanges)
	}
	if err := validateIncidentRequest(request); err != nil {
		t.Fatalf("a request with state changes must validate: %v", err)
	}
}

func TestHealthyBaselineResumesAfterRestartWithHistoricalFinding(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.FiringThreshold = 10
	before, err := NewController(config, []sdk.Detector{controllerDetector("health", time.Second, stateFinding("transient"))}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := before.Step(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := encodeControllerState(before.ExportState())
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeControllerState(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.History) == 0 || len(state.FindingStates) == 0 {
		t.Fatal("restart fixture lost historical finding")
	}
	after, err := NewController(config, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{}, sdk.Finding{}, sdk.Finding{})}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := after.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	baseline := &recordingBaseline{}
	after.Baseline = baseline
	for i := 1; i <= 3; i++ {
		if err := after.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(baseline.observations) == 0 || !baseline.observations[len(baseline.observations)-1] {
		t.Fatalf("historical finding blocked healthy baseline after restart: %v", baseline.observations)
	}
}
