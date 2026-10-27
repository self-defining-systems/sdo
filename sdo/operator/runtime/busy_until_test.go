package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
)

// Observers wait on busy_until alone, so it must be unset exactly when the
// controller owns no incident work, bounded by the controller's own limits, and
// anchored so polling it never moves the bound.
func TestBusyUntilIsUnsetWhenIdle(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	if got := exportState(t, controller).GetBusyUntil(); got != nil {
		t.Fatalf("idle controller published busy_until %v", got.AsTime())
	}
}

func TestBusyUntilCoversEveryQueuedReflection(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	controller.mu.Lock()
	controller.reflections.queued = []string{"first", "second"}
	controller.mu.Unlock()
	got := exportState(t, controller).GetBusyUntil()
	if got == nil {
		t.Fatal("two queued reflections are not published as busy")
	}
	// A failing reflection is retried MaxAttempts times, each up to the response timeout, then abandoned
	// in one more pass; the property is stated here without the controller's own formula.
	attempts := time.Duration(controller.config.ClosureRetry.MaxAttempts)
	if need := now.Add(2 * attempts * controller.config.ResponseTimeout); got.AsTime().Before(need) {
		t.Fatalf("busy_until %v ends before two failing reflections exhaust their retries (%v)", got.AsTime(), need)
	}
}

// Chained work: incident 2 opens after the window incident 1 anchored has lapsed, with no idle
// export between. The controller is busy, so busy_until must be in the future.
func TestBusyUntilReanchorsWhenNewWorkStartsAfterTheWindowLapsed(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	controller.mu.Lock()
	controller.reflections.queued = []string{"incident-1"}
	controller.mu.Unlock()
	first := exportState(t, controller).GetBusyUntil().AsTime()
	now = first.Add(time.Minute) // incident 1's whole window has lapsed and the controller never went idle
	controller.mu.Lock()
	controller.reflections.queued = []string{"incident-2"}
	controller.mu.Unlock()
	if got := exportState(t, controller).GetBusyUntil(); got == nil || !got.AsTime().After(now) {
		t.Fatalf("incident 2 is work in hand but busy_until %v is not after now %v", got, now)
	}
}

func TestBusyUntilCountsFindingsPendingInTheBatcher(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	if exportState(t, controller).GetBusyUntil() != nil {
		t.Fatal("an idle controller published busy_until")
	}
	controller.batcher.AddAt(stateFinding("fault"), now)
	if exportState(t, controller).GetBusyUntil() == nil {
		t.Fatal("a finding pending in the batcher, not yet an incident, is not published as busy")
	}
}

func TestBusyUntilDoesNotSlideWhileTheSameWorkContinues(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	controller.mu.Lock()
	controller.incidentOpen = true
	controller.mu.Unlock()
	first := exportState(t, controller).GetBusyUntil().AsTime()
	now = now.Add(20 * time.Minute)
	if second := exportState(t, controller).GetBusyUntil().AsTime(); !second.Equal(first) {
		t.Fatalf("busy_until slid from %v to %v while the incident stayed open", first, second)
	}
	controller.mu.Lock()
	controller.reflections.queued = []string{"late"}
	controller.mu.Unlock()
	if grown := exportState(t, controller).GetBusyUntil().AsTime(); !grown.After(first) {
		t.Fatalf("busy_until %v did not grow when a reflection joined the work (was %v)", grown, first)
	}
	controller.mu.Lock()
	controller.incidentOpen, controller.reflections.queued = false, nil
	controller.mu.Unlock()
	if exportState(t, controller).GetBusyUntil() != nil {
		t.Fatal("busy_until stayed set after all work finished")
	}
	now = now.Add(time.Hour)
	controller.mu.Lock()
	controller.incidentOpen = true
	controller.mu.Unlock()
	if again := exportState(t, controller).GetBusyUntil().AsTime(); !again.After(now) {
		t.Fatalf("new work reused the old anchor: %v", again)
	}
}

func TestBusyUntilCountsUnreadableQueueAndInFlightClosure(t *testing.T) {
	now := time.Now().UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	// The pending closure the helper installed is in flight.
	if exportState(t, controller).GetBusyUntil() == nil {
		t.Fatal("a pending closure is not published as busy")
	}
	controller.pendingClosure, controller.closureState = nil, 0
	controller.mu.Lock()
	controller.reflections.unknown = true
	controller.mu.Unlock()
	if exportState(t, controller).GetBusyUntil() == nil {
		t.Fatal("an unreadable reflection queue is not published as busy")
	}
}

// The Python eval waits on these exported states (tests/unit/benchmarks/.../test_busy_until_wait.py), so
// they are the controller's real ExportState, recorded with a fixed clock. A change to the bound that
// is not mirrored there fails here; regenerate with SDO_UPDATE_GOLDEN=1 and review the diff.
func TestBusyUntilExportedStatesForTheEvalWait(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	type sample struct {
		At    time.Time      `json:"at"`
		Note  string         `json:"note"`
		State map[string]any `json:"state"`
	}
	var samples []sample
	record := func(note string) {
		raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(exportState(t, controller))
		if err != nil {
			t.Fatal(err)
		}
		var state map[string]any
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, sample{At: now, Note: note, State: state})
	}
	queue := func(ids ...string) {
		controller.mu.Lock()
		controller.reflections.queued = ids
		controller.mu.Unlock()
	}
	queue("incident-1")
	record("incident 1's reflection is queued")
	first := exportState(t, controller).GetBusyUntil().AsTime()
	now = first.Add(time.Minute)
	queue("incident-2")
	record("incident 2 opened after incident 1's whole window lapsed; the controller never went idle")
	second := exportState(t, controller).GetBusyUntil().AsTime()
	now = second.Add(-time.Minute)
	queue()
	record("all work finished")

	got, err := json.MarshalIndent(samples, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "eval_wait", "chained_reflections.json")
	if os.Getenv("SDO_UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (generate with SDO_UPDATE_GOLDEN=1)", err)
	}
	if string(want) != string(got) {
		t.Fatalf("exported busy states changed; review and regenerate %s with SDO_UPDATE_GOLDEN=1", path)
	}
}
