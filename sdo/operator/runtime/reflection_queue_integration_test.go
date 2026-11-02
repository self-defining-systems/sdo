//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestFailedAttemptFollowUpDoesNotWaitForReflection(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repo, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repo, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
	workspace, err := native.PrepareIncident(ctx, "failed-attempt")
	if err != nil {
		t.Fatal(err)
	}
	closure := socialRepeatClosure(workspace)
	started, release := make(chan struct{}), make(chan struct{})
	native.Reflector = ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		close(started)
		<-release
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "per-cause proof unavailable"}, nil
	})
	now := time.Now()
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 3}, native, &now)
	controller.pendingClosure = &closure
	attemptClosure(t, controller)
	ack, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("failed attempt's closure was not acknowledgeable on its outcome commit")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.acknowledgmentResults)
	controller.processBrokerCompletions()
	if err := controller.driveQueuedReflections(ctx, now); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("queued reflection did not start")
	}
	// The reflection still runs, yet the incident's follow-up may dispatch.
	controller.mu.Lock()
	blocked := controller.closureBlocksNextLocked()
	controller.mu.Unlock()
	if blocked {
		t.Fatal("a running failed-attempt reflection blocks the follow-up dispatch")
	}
	state := exportState(t, controller)
	if ids := state.GetQueuedReflectionIncidentIds(); len(ids) != 1 || ids[0] != workspace.IncidentID {
		t.Fatalf("running reflection is not exported as open work: %v", ids)
	}
	close(release)
	awaitBrokerResult(t, controller.reflectionResults)
	controller.processBrokerCompletions()
	if ids := exportState(t, controller).GetQueuedReflectionIncidentIds(); len(ids) != 0 {
		t.Fatalf("finished reflection still exported as open work: %v", ids)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.Acknowledged || !ledger.ReflectionProcessed || ledger.GetClosurePhaseTimestamps()["reflection_committed"] == nil {
		t.Fatalf("queued reflection not durably finished: %v", ledger)
	}
}

func TestSuccessfulClosureStillReflectsBeforeItsReceipt(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repo, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repo, LedgerRoot: t.TempDir(), Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
		return DerivedOutcome{Record: &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: closure.Request.IncidentID, Classification: "success"}, RequiresReflection: true}, nil
	}), ResponderBackend: "test", ResponderModel: "test", ForceReflection: true, Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "nothing reusable"}, nil
	})}
	workspace, err := native.PrepareIncident(ctx, "success")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := native.ProcessClosure(ctx, socialRepeatClosure(workspace)); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !ledger.ReflectionProcessed || ledger.ReflectionQueued || ledger.Receipt == nil {
		t.Fatalf("success closure must reflect before its receipt: calls=%d ledger=%v", calls, ledger)
	}
	if queued, err := native.QueuedReflections(); err != nil || len(queued) != 0 {
		t.Fatalf("success reflection queued: %v %v", queued, err)
	}
}

// A failed ledger rescan must not export an empty queue: admission would read
// it as idle while a reflection is still queued.
func TestQueuedReflectionRescanFailureKeepsWorkOpen(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repo, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ledgerRoot := t.TempDir()
	native := NativeIncidentBroker{Repository: repo, LedgerRoot: ledgerRoot, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
	now := time.Now()
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 3}, native, &now)
	if err := os.WriteFile(filepath.Join(ledgerRoot, "corrupt.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := controller.driveQueuedReflections(ctx, now); err != nil {
		t.Fatal(err)
	}
	if !exportState(t, controller).GetQueuedReflectionsUnknown() || !controller.reflectionWorkOpen() {
		t.Fatal("an unreadable reflection queue is exported as idle")
	}
}
