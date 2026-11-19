package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// preparingDispatcher is a dispatcher with a PrepareDispatch step whose result
// the test scripts. It never starts a worker: Dispatch records the call.
type preparingDispatcher struct {
	mu       sync.Mutex
	prepare  func(call int) error
	prepares int
	started  int
}

func (d *preparingDispatcher) PrepareDispatch(_ context.Context, request IncidentRequest) (IncidentRequest, error) {
	d.mu.Lock()
	d.prepares++
	call := d.prepares
	d.mu.Unlock()
	if err := d.prepare(call); err != nil {
		return IncidentRequest{}, err
	}
	request.PreparedAgentTask = `{"task":"frozen"}`
	return request, nil
}

func (d *preparingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.started++
	d.mu.Unlock()
	return completedResult(request.IncidentID), nil
}

func (d *preparingDispatcher) prepareCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.prepares
}

// memoryStateStore is a compare-and-swap StateStore in memory. A save fails
// while failSaves is set, the way a store outage does.
type memoryStateStore struct {
	mu        sync.Mutex
	state     *contracts.ControllerState
	revision  int
	saves     int
	failSaves bool
}

func (s *memoryStateStore) Load(context.Context) (*contracts.ControllerState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return nil, "", nil
	}
	return proto.Clone(s.state).(*contracts.ControllerState), fmt.Sprint(s.revision), nil
}

func (s *memoryStateStore) Save(_ context.Context, state *contracts.ControllerState, expected string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSaves {
		return "", errors.New("synthetic store outage")
	}
	if s.state != nil && expected != fmt.Sprint(s.revision) {
		return "", ErrStateConflict
	}
	s.state = proto.Clone(state).(*contracts.ControllerState)
	s.revision++
	s.saves++
	return fmt.Sprint(s.revision), nil
}

// preparationHarness is a controller with one open incident whose dispatch is
// pending and not yet prepared, a fake clock, a state store and a lifecycle sink.
type preparationHarness struct {
	t          *testing.T
	controller *Controller
	dispatcher *preparingDispatcher
	store      *memoryStateStore
	lifecycle  *recordingLifecycle
	now        time.Time
}

func newPreparationHarness(t *testing.T, prepare func(call int) error) *preparationHarness {
	t.Helper()
	detected := time.Unix(100, 0).UTC()
	h := &preparationHarness{t: t, now: detected, dispatcher: &preparingDispatcher{prepare: prepare}, store: &memoryStateStore{}, lifecycle: &recordingLifecycle{}}
	config := testControllerConfig()
	config.ResponseTimeout = 25 * time.Minute
	health := &twoFaultHealth{}
	health.first.Store(true)
	controller, err := NewController(config, []sdk.Detector{health}, staticProvider{}, h.dispatcher, detected)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return h.now }
	controller.stateStore = h.store
	controller.SetLifecycleSink(h.lifecycle)
	// A real detector opens a real, valid incident.
	for range 20 {
		if err := controller.Step(context.Background(), h.now, nil); err != nil {
			t.Fatal(err)
		}
		controller.mu.Lock()
		open := controller.incidentOpen
		controller.mu.Unlock()
		if open {
			break
		}
		h.now = h.now.Add(time.Second)
	}
	controller.mu.Lock()
	open, state, request := controller.incidentOpen, controller.dispatchState, controller.currentIncidentRequest
	controller.mu.Unlock()
	if !open || state != contracts.DispatchState_DISPATCH_STATE_PENDING || request == nil || request.PreparedAgentTask != "" {
		t.Fatalf("incident open = %v, dispatch %v, request %+v; want an open incident with a pending unprepared dispatch", open, state, request)
	}
	h.controller = controller
	return h
}

// step is one pump of the run loop's effects: prepare, save, then the dispatch
// effect when it is ready. The save must always succeed.
func (h *preparationHarness) step() {
	h.t.Helper()
	ctx := context.Background()
	if prepared, err := h.controller.PrepareDispatchStep(ctx); err != nil {
		h.t.Fatalf("PrepareDispatchStep: %v", err)
	} else if prepared {
		if err := h.controller.PersistState(ctx); err != nil {
			h.t.Fatalf("PersistState after preparing: %v", err)
		}
	}
	if err := h.controller.PersistState(ctx); err != nil {
		h.t.Fatalf("PersistState: %v", err)
	}
}

func (h *preparationHarness) escalated() bool {
	h.controller.mu.Lock()
	defer h.controller.mu.Unlock()
	return !h.controller.escalatedAt.IsZero()
}

func (h *preparationHarness) dispatchFailed() *contracts.LifecycleEvent {
	h.lifecycle.mu.Lock()
	defer h.lifecycle.mu.Unlock()
	for _, event := range h.lifecycle.events {
		if event.GetEvent() == PhaseDispatchFailed {
			return event
		}
	}
	return nil
}

// A permanent preparation failure (memory the reader rejects) escalates in one
// step: one PrepareDispatch call however many steps follow, a state save that
// succeeds every step, and no agent.
func TestPermanentPreparationErrorEscalatesWithoutRetry(t *testing.T) {
	h := newPreparationHarness(t, func(int) error {
		return fmt.Errorf("read logical operational memory: %w: unrecognized file in operational memory: .sdo/playbooks/stray-x", memory.ErrUnreadable)
	})
	for range 8 {
		h.step()
		h.now = h.now.Add(4 * time.Second)
	}
	if calls := h.dispatcher.prepareCalls(); calls != 1 {
		t.Fatalf("PrepareDispatch called %d times, want exactly 1 for a permanent error", calls)
	}
	if !h.escalated() {
		t.Fatal("the incident did not escalate")
	}
	if _, ok := h.controller.PendingDispatchEffect(); ok {
		t.Fatal("an escalated dispatch must not leave a pending dispatch effect")
	}
	event := h.dispatchFailed()
	if event == nil {
		t.Fatal("no responder_dispatch_failed lifecycle event")
	}
	if event.GetReason() == "" || !contains(event.GetReason(), "permanent") || !contains(event.GetReason(), "unrecognized file") {
		t.Fatalf("responder_dispatch_failed reason = %q; want the class and the cause", event.GetReason())
	}
	state, _, err := h.store.Load(context.Background())
	if err != nil || state == nil {
		t.Fatalf("no saved state: %v", err)
	}
	if state.GetDispatchState() != contracts.DispatchState_DISPATCH_STATE_COMPLETED || state.GetEscalatedAt() == nil {
		t.Fatalf("saved state = dispatch %v escalated %v; want completed and escalated", state.GetDispatchState(), state.GetEscalatedAt())
	}
}

// A transient preparation failure backs off, then escalates when
// response.dispatch_prepare_bound passes, never before.
func TestTransientPreparationErrorBacksOffThenEscalates(t *testing.T) {
	h := newPreparationHarness(t, func(int) error { return errors.New("synthetic git error") })
	var calls []time.Duration
	start := h.now
	for h.now.Sub(start) < 3*time.Minute && !h.escalated() {
		before := h.dispatcher.prepareCalls()
		h.step()
		if h.dispatcher.prepareCalls() > before {
			calls = append(calls, h.now.Sub(start))
		}
		h.now = h.now.Add(time.Second)
	}
	if !h.escalated() {
		t.Fatalf("still not escalated after %s of failing preparation; calls at %v", h.now.Sub(start), calls)
	}
	if got := calls[len(calls)-1]; got < 2*time.Minute || got > 2*time.Minute+5*time.Second {
		t.Fatalf("escalated at %s, want at the 2m dispatch_prepare_bound; calls at %v", got, calls)
	}
	if len(calls) < 4 || len(calls) > 12 {
		t.Fatalf("%d preparation attempts in 2m; want bounded exponential backoff (a handful), calls at %v", len(calls), calls)
	}
	for i := 2; i < len(calls)-1; i++ {
		if calls[i]-calls[i-1] < calls[i-1]-calls[i-2] {
			t.Fatalf("backoff shrank: calls at %v", calls)
		}
	}
	if event := h.dispatchFailed(); event == nil || !contains(event.GetReason(), "transient") {
		t.Fatalf("responder_dispatch_failed = %v; want a transient-class reason", event)
	}
}

// A transient failure that clears is not an escalation: the task freezes, is
// saved, and the dispatch effect becomes ready.
func TestTransientPreparationErrorThatClearsDispatches(t *testing.T) {
	h := newPreparationHarness(t, func(call int) error {
		if call < 3 {
			return errors.New("synthetic api error")
		}
		return nil
	})
	for range 30 {
		h.step()
		h.now = h.now.Add(time.Second)
	}
	if h.escalated() {
		t.Fatal("a transient failure that cleared must not escalate")
	}
	effect, ok := h.controller.PendingDispatchEffect()
	if !ok || effect.Request.PreparedAgentTask == "" {
		t.Fatalf("dispatch effect = %+v, %v; want a ready effect with the frozen task", effect, ok)
	}
	if calls := h.dispatcher.prepareCalls(); calls != 3 {
		t.Fatalf("PrepareDispatch called %d times, want 3", calls)
	}
}

// The state save never depends on preparation: with a failing dispatcher the
// store still gets every save.
func TestStateSaveDoesNotPrepareDispatch(t *testing.T) {
	h := newPreparationHarness(t, func(int) error { return errors.New("synthetic failure") })
	if err := h.controller.PersistState(context.Background()); err != nil {
		t.Fatalf("PersistState with a failing dispatcher preparation: %v", err)
	}
	if calls := h.dispatcher.prepareCalls(); calls != 0 {
		t.Fatalf("PersistState prepared the dispatch (%d calls); preparation is a controller step", calls)
	}
}

func contains(text, part string) bool { return strings.Contains(text, part) }

// After a dispatch that could not be prepared escalates, the incident closes
// like any escalated one when its alerts clear: through the broker, marked
// escalated (so nothing is learned), never as a dispatch failure.
func TestRecoveredAfterEscalatedDispatchClosesNormally(t *testing.T) {
	runOnOneProcessor(t)
	ctx := context.Background()
	opened := time.Unix(20_000, 0).UTC()
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	health := &twoFaultHealth{}
	health.first.Store(true)
	dispatcher := &preparingDispatcher{prepare: func(int) error { return fmt.Errorf("%w: synthetic unreadable memory", memory.ErrUnreadable) }}
	controller, err := NewController(cfg, []sdk.Detector{health}, staticProvider{}, dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	now := opened
	controller.now = func() time.Time { return now }
	controller.gitRead = fakeArchitecture("")
	broker := &riskBroker{}
	controller.broker = broker
	lifecycle := &recordingLifecycle{}
	controller.SetLifecycleSink(lifecycle)
	cleared := false
	for step := 0; step < 600 && len(broker.closures()) == 0; step++ {
		if err := controller.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := controller.PendingWorkspaceEffect(); ok {
			if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-controller.workspaceResults
			controller.workspaceResults <- completion
		} else if prepared, err := controller.PrepareDispatchStep(ctx); err != nil {
			t.Fatal(err)
		} else if prepared {
			if err := controller.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
		} else if effect, ok := controller.PendingDispatchEffect(); ok {
			t.Fatalf("a dispatch whose task cannot be prepared became ready: %+v", effect)
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		}
		if lifecycle.has(PhaseDispatchFailed) && !cleared {
			health.first.Store(false)
			cleared = true
		}
		now = now.Add(time.Second)
	}
	closures := broker.closures()
	if len(closures) == 0 {
		t.Fatal("the incident never closed after its alerts cleared")
	}
	closure := closures[0]
	if closure.EscalatedAt.IsZero() {
		t.Fatal("the closure is not marked escalated; an escalated close must never learn")
	}
	if closure.DispatchError != "" {
		t.Fatalf("closure dispatch error = %q; an escalated dispatch failure closes by health, not as a failed Job", closure.DispatchError)
	}
	if got := dispatcher.prepareCalls(); got != 1 {
		t.Fatalf("PrepareDispatch called %d times, want 1", got)
	}
}

func (r *recordingLifecycle) has(phase string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, event := range r.events {
		if event.GetEvent() == phase {
			return true
		}
	}
	return false
}

// The agent stage freezes its own hand-off inside the response pipeline; a
// preparation failure there follows the same policy as the controller's step.
func TestStageHandoffPreparationFailureFollowsThePolicy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		wantEscalated bool
	}{
		{"permanent", fmt.Errorf("%w: synthetic", memory.ErrSnapshotBudget), true},
		{"transient", errors.New("synthetic api error"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPreparationHarness(t, func(int) error { return nil })
			h.controller.mu.Lock()
			h.controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
			h.controller.incidentDispatchedAt = h.now
			id := h.controller.currentIncidentRequest.IncidentID
			h.controller.mu.Unlock()
			h.controller.handleDispatchCompletion(dispatchCompletion{incidentID: id, err: &DispatchPrepareError{Err: tc.err}}, h.now)
			if got := h.escalated(); got != tc.wantEscalated {
				t.Fatalf("escalated = %v, want %v", got, tc.wantEscalated)
			}
			h.controller.mu.Lock()
			state, retryAt := h.controller.dispatchState, h.controller.dispatchNextRetryAt
			h.controller.mu.Unlock()
			if tc.wantEscalated && state != contracts.DispatchState_DISPATCH_STATE_COMPLETED {
				t.Fatalf("dispatch state = %v, want completed", state)
			}
			if !tc.wantEscalated && (state != contracts.DispatchState_DISPATCH_STATE_PENDING || !retryAt.After(h.now)) {
				t.Fatalf("dispatch state = %v, retry at %v; want pending with a backoff", state, retryAt)
			}
		})
	}
}
