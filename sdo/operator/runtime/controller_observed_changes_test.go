package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// scriptedBaseline returns the diff each step's time selects.
type scriptedBaseline struct {
	changes func(now time.Time) []StateChange
}

func (b *scriptedBaseline) Observe(time.Time, bool) {}

func (b *scriptedBaseline) Changes(now time.Time) *StateChanges {
	return &StateChanges{BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: now.UTC(), Changes: b.changes(now)}
}

var (
	selectorFault = StateChange{Kind: "Service", Name: "frontend", Change: StateChangeModified}
	lateFault     = StateChange{Kind: "Deployment", Name: "frontend", Change: StateChangeModified}
)

// faultTimeline: the selector fault is in the dispatch diff (t=1), a late
// component lands at t=2 while the incident is open, and both are reverted
// exactly from t=3, so neither is in the closing view.
func faultTimeline(now time.Time) []StateChange {
	switch {
	case now.Before(time.Unix(2, 0)):
		return []StateChange{selectorFault}
	case now.Before(time.Unix(3, 0)):
		return []StateChange{selectorFault, lateFault}
	default:
		return []StateChange{}
	}
}

func observedChangesController(t *testing.T, baseline StateBaseline) (*Controller, *recordingDispatcher) {
	t.Helper()
	cause := controllerDetector(
		"cause", time.Second, stateFinding("cause"), stateFinding("cause"),
		sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{},
	)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{cause}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.Baseline = baseline
	return controller, dispatcher
}

// TestClosureRecordsEveryChangeObservedWhileTheIncidentWasOpen covers the
// controller fact behind rc2's evidence-timing check: a state-change
// citation is evidence only if the controller saw that change before the
// responder's repair of it. The closure lists every object seen changed
// while the incident was open with its first observation, including a late
// component the responder reverted exactly and the closing view no longer
// shows.
func TestClosureRecordsEveryChangeObservedWhileTheIncidentWasOpen(t *testing.T) {
	controller, dispatcher := observedChangesController(t, &scriptedBaseline{changes: faultTimeline})
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
	var closure IncidentClosure
	for sample := 2; sample < 7; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("step %d: %v", sample, err)
		}
		select {
		case closure = <-closed:
			sample = 7
		default:
		}
	}
	if closure.Request.IncidentID == "" {
		t.Fatal("verified incident did not emit closure")
	}

	want := []ObservedStateChange{
		{Kind: "Service", Name: "frontend", FirstObservedAt: time.Unix(1, 0).UTC()},
		{Kind: "Deployment", Name: "frontend", FirstObservedAt: time.Unix(2, 0).UTC()},
	}
	if len(closure.ObservedStateChanges) != len(want) {
		t.Fatalf("observed changes = %+v, want %+v", closure.ObservedStateChanges, want)
	}
	for index, entry := range want {
		got := closure.ObservedStateChanges[index]
		if got.Kind != entry.Kind || got.Name != entry.Name || !got.FirstObservedAt.Equal(entry.FirstObservedAt) {
			t.Fatalf("observed change %d = %+v, want %+v", index, got, entry)
		}
	}
	if closure.FinalStateChanges == nil || len(closure.FinalStateChanges.Changes) != 0 {
		t.Fatalf("both faults were reverted; the closing view must be empty: %+v", closure.FinalStateChanges)
	}
	encoded, err := json.Marshal(closure)
	if err != nil {
		t.Fatalf("marshal closure: %v", err)
	}
	if !strings.Contains(string(encoded), `"observed_state_changes":[`) {
		t.Fatalf("closure JSON must carry observed_state_changes: %s", encoded)
	}
}

// TestObservedChangesSurviveARestart keeps first observations exact across a
// controller restart mid-incident: restoring must not move a late fault's
// first observation after the responder's repair.
func TestObservedChangesSurviveARestart(t *testing.T) {
	controller, dispatcher := observedChangesController(t, &scriptedBaseline{changes: faultTimeline})
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("late step: %v", err)
	}
	state := controller.ExportState()
	if len(state.IncidentObservedChanges) != 2 {
		t.Fatalf("exported observed changes = %+v", state.IncidentObservedChanges)
	}

	restarted, _ := observedChangesController(t, &scriptedBaseline{changes: faultTimeline})
	if err := restarted.RestoreState(state); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored := restarted.ExportState().IncidentObservedChanges
	if len(restored) != 2 || !restored[1].FirstObservedAt.Equal(time.Unix(2, 0).UTC()) {
		t.Fatalf("restored observed changes = %+v", restored)
	}
}

// TestNoBaselineRecordsNoObservedChanges: without a baseline, state-change
// evidence cannot be checked, so the closure must not claim that nothing
// changed.
func TestNoBaselineRecordsNoObservedChanges(t *testing.T) {
	controller, dispatcher := observedChangesController(t, nil)
	controller.Baseline = nil
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
	for sample := 2; sample < 7; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("step %d: %v", sample, err)
		}
	}
	closure := <-closed
	encoded, err := json.Marshal(closure)
	if err != nil {
		t.Fatalf("marshal closure: %v", err)
	}
	if strings.Contains(string(encoded), "observed_state_changes") {
		t.Fatalf("a closure without a baseline must omit observed_state_changes: %s", encoded)
	}
}
