//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Agent-free loop through the real broker: a verified repair whose source
// fails deterministic validation is submitted once and ends the closure.
func TestNativeClosureWithInvalidRepairIsNotResubmitted(t *testing.T) {
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "success", false), nil
		}),
	}
	counting := &countingIncidentBroker{IncidentBroker: native}
	now := time.Unix(1000, 0).UTC()
	controller := newClosureRetryController(t, ClosureRetryPolicy{MaxAttempts: 8, InitialBackoff: time.Second, MaxBackoff: 9 * time.Second}, counting, &now)
	workspace, err := native.PrepareIncident(context.Background(), controller.pendingClosure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "service.yaml"), []byte("kind: Service\nmetadata:\n  name: a\n  name: b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	controller.pendingClosure = &closure
	for step := 0; step < 8; step++ {
		if _, due := controller.PendingClosureEffect(); due {
			attemptClosure(t, controller)
		}
		now = now.Add(10 * time.Second)
	}
	state := exportState(t, controller)
	if counting.calls != 1 || state.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED || !strings.Contains(state.ClosureFailure.GetLastError(), "validation diagnostic") {
		t.Fatalf("invalid repair closure: calls=%d state=%s failure=%v", counting.calls, state.ClosureState, state.ClosureFailure)
	}
}
