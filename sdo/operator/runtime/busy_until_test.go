package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/sdk"
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

// Persisted reflection state is output only. A controller restored from an export that listed queued or
// unreadable reflections owns none of that work: the broker ledger is the one source, rescanned at start,
// so a restart cannot carry stale queue state into busy_until.
func TestBusyUntilIgnoresRestoredReflectionState(t *testing.T) {
	now := time.Now().UTC()
	old := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	old.pendingClosure, old.closureState = nil, 0
	old.mu.Lock()
	old.reflections.queued, old.reflections.unknown = []string{"gone"}, true
	old.mu.Unlock()
	exported := exportState(t, old)
	if exported.GetBusyUntil() == nil || len(exported.GetQueuedReflectionIncidentIds()) != 1 || !exported.GetQueuedReflectionsUnknown() {
		t.Fatal("setup: the old controller should export queued and unreadable reflections as busy")
	}
	restarted := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	restarted.pendingClosure, restarted.closureState = nil, 0
	if err := restarted.RestoreState(exported); err != nil {
		t.Fatal(err)
	}
	if got := exportState(t, restarted).GetBusyUntil(); got != nil {
		t.Fatalf("restored queue state with no running task published busy_until %v", got.AsTime())
	}
}

// Findings are not work. A finding the controller holds as active (for example drift on objects absent
// from accepted source) neither opens an incident nor gates closure, so a controller with nothing open,
// nothing batched and nothing queued is idle however many findings stay active.
func TestBusyUntilIgnoresActiveFindingsThatOwnNoWork(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	controller.pendingClosure, controller.closureState = nil, 0
	drift := stateFinding("drift.unexplained")
	for pass := 0; pass < 3; pass++ {
		controller.tracker.Observe(now.Add(time.Duration(pass)*time.Second), drift.DetectorID, []sdk.Finding{drift})
	}
	if controller.tracker.Quiet() {
		t.Fatal("setup: the tracker should hold an active finding")
	}
	if err := controller.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if got := exportState(t, controller).GetBusyUntil(); got != nil {
		t.Fatalf("an active finding with no incident, batch, closure or reflection published busy_until %v", got.AsTime())
	}
	// The Python eval wait reads this export (test_lane_resource_admission.py): the producer feeds the consumer.
	assertEvalWaitGolden(t, "active_findings_idle.json", []evalWaitSample{
		recordEvalWaitSample(t, controller, now, "drift findings stay active; nothing is open, batched or queued"),
	})
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
	var samples []evalWaitSample
	record := func(note string) {
		samples = append(samples, recordEvalWaitSample(t, controller, now, note))
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

	assertEvalWaitGolden(t, "chained_reflections.json", samples)
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

func assertEvalWaitGolden(t *testing.T, name string, samples []evalWaitSample) {
	t.Helper()
	got, err := json.MarshalIndent(samples, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "eval_wait", name)
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
