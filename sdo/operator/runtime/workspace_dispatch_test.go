package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A workspace finishing during an observation can start dispatch at the end of
// that observation. No other detector pass is required to consume its result.
func TestReadyWorkspaceDispatchesWithoutAnotherObservation(t *testing.T) {
	ctx := context.Background()
	h := newPreparationHarness(t, func(int) error { return nil })
	c := h.controller
	request := c.currentIncidentRequest
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING
	c.workspaceResults <- workspaceCompletion{workspace: IncidentWorkspace{
		IncidentID: request.IncidentID, Worktree: "/proposal", BaseCommit: request.SourceCommit,
	}}
	if err := executeStartupEffects(ctx, c, true); err != nil {
		t.Fatal(err)
	}
	if h.dispatcher.prepareCalls() != 0 {
		t.Fatal("maintenance pause let the responder prepare a task")
	}
	if err := executePendingEffects(ctx, c); err != nil {
		t.Fatal(err)
	}
	awaitEffectGoroutines(c)
	state, _, err := h.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.GetIncidentRequest(); got.GetRepositoryWorktree() != "/proposal" ||
		got.GetRepositoryBaseCommit() != request.SourceCommit || got.GetPreparedAgentTask() == "" {
		t.Fatalf("durable dispatch request = %v, want prepared task bound to the completed workspace", got)
	}
	if err := executePendingEffects(ctx, c); err != nil {
		t.Fatal(err)
	}
	awaitEffectGoroutines(c)
	if got := h.dispatcher.started; got != 1 {
		t.Fatalf("worker launches = %d, want one", got)
	}
}
