package runtime

import (
	"testing"
	"time"
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
	perReflection := controller.config.ResponseTimeout + 182*time.Second
	got := exportState(t, controller).GetBusyUntil()
	if got == nil {
		t.Fatal("two queued reflections are not published as busy")
	}
	if need := now.Add(2 * perReflection); got.AsTime().Before(need) {
		t.Fatalf("busy_until %v ends before two reflections can finish or be abandoned (%v)", got.AsTime(), need)
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
