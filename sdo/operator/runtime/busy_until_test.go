package runtime

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/sdk"
)

// A finding that has fired but not yet confirmed is not in the batcher or the tracker's active set, so
// it is not dispatchable work: it is reported idle, by decision (REV39 P2-a). It converts to a batched
// finding within one confirmation interval, and admission separately proves the detectors clear.
func TestBusyUntilIgnoresFindingsStillPendingConfirmation(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	controller.tracker.SetPolicy("fault", 2, 2, 0)
	fault := stateFinding("fault")
	controller.tracker.Observe(now, fault.DetectorID, []sdk.Finding{fault})
	states := exportState(t, controller).GetFindingStates()
	if len(states) != 1 {
		t.Fatalf("setup: want one tracked finding, got %v", states)
	}
	for _, state := range states {
		if state.GetActive() || state.GetFiringCount() != 1 {
			t.Fatalf("setup: want a finding fired once and not yet active, got %v", state)
		}
	}
	if got := exportState(t, controller).GetBusyUntil(); got != nil {
		t.Fatalf("a finding pending confirmation, owned by no task, published busy_until %v", got.AsTime())
	}
	// Confirmation activates it and the controller batches it in the same pass: now it is work.
	controller.tracker.Observe(now.Add(time.Second), fault.DetectorID, []sdk.Finding{fault})
	controller.batcher.AddAt(fault, now.Add(time.Second))
	if exportState(t, controller).GetBusyUntil() == nil {
		t.Fatal("a confirmed finding in the batcher is not published as busy")
	}
}

// evalWaitSample is one recorded controller export for the Python eval wait.
type evalWaitSample struct {
	At    time.Time      `json:"at"`
	Note  string         `json:"note"`
	State map[string]any `json:"state"`
}

func recordEvalWaitSample(t *testing.T, controller *Controller, at time.Time, note string) evalWaitSample {
	t.Helper()
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(exportState(t, controller))
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	return evalWaitSample{At: at, Note: note, State: state}
}

