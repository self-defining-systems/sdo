package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type rolloutRecordingBroker struct {
	recordingIncidentBroker
	pending bool
}

func (b *rolloutRecordingBroker) RolloutPending(ClosureReceipt) (bool, error) { return b.pending, nil }

func TestAcknowledgmentWaitsForRestartedDetectorRevision(t *testing.T) {
	start := time.Unix(0, 0)
	controller, err := NewController(testControllerConfig(), []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	receipt := ClosureReceipt{IncidentID: "rollout-1", OutcomeCommit: "outcome", ReflectionCommit: "learned", AckToken: "ack"}
	controller.closureState = "committed"
	controller.closureReceipt = &receipt
	broker := &rolloutRecordingBroker{pending: true}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	effect, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("durable ACK was not pending")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); !errors.Is(err, ErrControllerRolloutRestartRequired) {
		t.Fatalf("expected restart handoff, got %v", err)
	}
	if controller.closureState != "committed" || len(broker.calls()) != 0 {
		t.Fatal("ACK began before detector rollout")
	}
	broker.pending = false
	if err := controller.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.acknowledgmentResults)
	if calls := broker.calls(); len(calls) != 1 || calls[0] != "ack:rollout-1" {
		t.Fatalf("ACK calls after rollout: %v", calls)
	}
}

type recordingIncidentBroker struct {
	mu         sync.Mutex
	workspace  IncidentWorkspace
	receipt    ClosureReceipt
	operations []string
}

func (b *recordingIncidentBroker) PrepareIncident(_ context.Context, incidentID string) (IncidentWorkspace, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, "prepare:"+incidentID)
	workspace := b.workspace
	workspace.IncidentID = incidentID
	return workspace, nil
}

func (b *recordingIncidentBroker) ProcessClosure(_ context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, "process:"+closure.Request.IncidentID)
	receipt := b.receipt
	receipt.IncidentID = closure.Request.IncidentID
	return receipt, nil
}

func (b *recordingIncidentBroker) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, "ack:"+receipt.IncidentID)
	return nil
}

func (b *recordingIncidentBroker) ReleaseIncident(_ context.Context, incidentID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.operations = append(b.operations, "release:"+incidentID)
	return nil
}

func (b *recordingIncidentBroker) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.operations...)
}

func TestWorkspacePreparationIsPersistedAndReplayableBeforeDispatch(t *testing.T) {
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	detector := controllerDetector("fault", time.Second, stateFinding("fault"))
	broker := &recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base-sha"}}
	first, err := NewController(
		config,
		[]sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := first.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	if err := first.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("produce workspace effect: %v", err)
	}
	effect, ok := first.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("expected workspace preparation before dispatch")
	}
	if _, ok := first.PendingDispatchEffect(); ok {
		t.Fatal("dispatch became visible before worktree preparation")
	}

	restored, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(first.ExportState()); err != nil {
		t.Fatalf("restore workspace effect: %v", err)
	}
	if err := restored.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set restored broker: %v", err)
	}
	// A crash can restore an activation that was already represented by the
	// closure. It must not become a duplicate incident after acknowledgment.
	restored.batcher.Add(stateFinding("fault"))
	replayed, ok := restored.PendingWorkspaceEffect()
	if !ok || replayed != effect {
		t.Fatalf("workspace effect changed across restart: got %#v want %#v", replayed, effect)
	}
	if err := restored.ExecuteWorkspaceEffect(context.Background(), replayed); err != nil {
		t.Fatalf("execute workspace effect: %v", err)
	}
	awaitBrokerResult(t, restored.workspaceResults)
	restored.processBrokerCompletions()
	dispatch, ok := restored.PendingDispatchEffect()
	if !ok {
		t.Fatal("prepared workspace did not release dispatch")
	}
	if dispatch.Request.RepositoryWorktree != "/worktrees/incident" ||
		dispatch.Request.RepositoryBaseCommit != "base-sha" {
		t.Fatalf("prepared workspace was not persisted in request: %#v", dispatch.Request)
	}
	if got := broker.calls(); len(got) != 1 || got[0] != "prepare:"+effect.IncidentID {
		t.Fatalf("workspace preparation was not exactly once: %v", got)
	}
}

func TestClosureReceiptPersistsBeforeIdempotentAcknowledgmentClearsClosure(t *testing.T) {
	start := time.Unix(0, 0)
	controller, err := NewController(
		testControllerConfig(),
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	request := controller.incidentRequest(start, []sdk.Finding{stateFinding("fault")})
	request.DetectorHistory = testDetectorHistory(start)
	closure := IncidentClosure{
		Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)),
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}},
		DetectedAt:          start, DispatchedAt: start, ResponderCompletedAt: start, VerifiedAt: start,
	}
	controller.pendingClosure = &closure
	controller.closureState = "pending"
	broker := &recordingIncidentBroker{receipt: ClosureReceipt{
		Worktree: "/worktrees/incident", BaseCommit: "base", ProposalCommit: "repair",
		OutcomeCommit: "outcome", AckToken: "token",
	}}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}

	effect, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("expected persisted closure effect")
	}
	if err := controller.ExecuteClosureEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute closure: %v", err)
	}
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	state := controller.ExportState()
	if state.PendingClosure == nil || state.ClosureReceipt == nil || state.ClosureState != "committed" {
		t.Fatalf("closure receipt was not durable before acknowledgment: %#v", state)
	}
	if controller.CurrentAcceptedCommit() != "outcome" {
		t.Fatalf("controller retained stale accepted commit: %s", controller.CurrentAcceptedCommit())
	}

	restored, err := NewController(
		testControllerConfig(),
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatalf("restore closure receipt: %v", err)
	}
	if err := restored.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set restored broker: %v", err)
	}
	ack, ok := restored.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("restored receipt did not produce acknowledgment effect")
	}
	restored.batcher.Add(stateFinding("fault"))
	if err := restored.ExecuteClosureAcknowledgmentEffect(context.Background(), ack); err != nil {
		t.Fatalf("execute acknowledgment: %v", err)
	}
	awaitBrokerResult(t, restored.acknowledgmentResults)
	restored.processBrokerCompletions()
	if _, ok := restored.PendingIncidentClosure(); ok {
		t.Fatal("closure cleared before or remained after durable broker acknowledgment")
	}
	if pending := restored.batcher.Snapshot().Findings; len(pending) != 0 {
		t.Fatalf("acknowledgment left stale findings eligible for duplicate dispatch: %#v", pending)
	}
	acknowledged := restored.ExportState()
	if acknowledged.LastAcknowledgedIncidentID != request.IncidentID {
		t.Fatalf("acknowledged incident was not durable: %#v", acknowledged)
	}
	recovered, err := NewController(
		testControllerConfig(),
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new acknowledged-state controller: %v", err)
	}
	if err := recovered.RestoreState(acknowledged); err != nil {
		t.Fatalf("restore acknowledged closure: %v", err)
	}
	if recovered.LastAcknowledgedIncidentID() != request.IncidentID {
		t.Fatal("replacement controller lost durable acknowledged-closure marker")
	}
	if got := broker.calls(); len(got) != 2 || got[0] != "process:"+request.IncidentID ||
		got[1] != "ack:"+request.IncidentID {
		t.Fatalf("closure ordering changed: %v", got)
	}
}

func awaitBrokerResult[T any](t *testing.T, channel <-chan T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if len(channel) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for broker result")
}

func cloneIncidentResultPointer(result IncidentResult) *IncidentResult {
	return cloneIncidentResult(&result)
}
