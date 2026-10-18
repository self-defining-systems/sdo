package runtime

import (
	"context"
	"testing"
)

// drainWorkspace executes the pending workspace preparation and processes its
// completion, so the prepared worktree is recorded on the open incident.
func drainWorkspace(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("expected a pending workspace effect")
	}
	if err := controller.ExecuteWorkspaceEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute workspace effect: %v", err)
	}
	awaitBrokerResult(t, controller.workspaceResults)
	controller.processBrokerCompletions()
}

// drainRelease executes the pending worktree release and processes its
// completion.
func drainRelease(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingReleaseEffect()
	if !ok {
		t.Fatal("expected a pending release effect")
	}
	if err := controller.ExecuteReleaseEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute release effect: %v", err)
	}
	awaitBrokerResult(t, controller.releaseResults)
	controller.processBrokerCompletions()
}

// completeFirstResponderWithBroker drives the original incident through
// broker-backed worktree preparation, dispatch, and responder completion, the
// way a persistent controller does when a broker is attached.
func completeFirstResponderWithBroker(
	t *testing.T, controller *Controller, dispatcher *summarizingDispatcher,
) IncidentRequest {
	t.Helper()
	stepAt(t, controller, 0)
	stepAt(t, controller, 1)
	drainWorkspace(t, controller)
	executePendingEffect(t, controller)
	first := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	stepAt(t, controller, 2)
	return first
}

func countReleases(calls []string, incidentID string) int {
	count := 0
	for _, call := range calls {
		if call == "release:"+incidentID {
			count++
		}
	}
	return count
}

// TestControllerReleasesSupersededParentWorktree is the production-side
// counterpart to the adapter-side orphan reap: when a follow-up supersedes an
// open incident in place, the parent's prepared worktree is never closed and
// so was leaked. The controller must release it at the source, exactly once,
// and never touch the live follow-up's own worktree.
func TestControllerReleasesSupersededParentWorktree(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 2, healthBPersistent())
	broker := &recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base"}}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}

	first := completeFirstResponderWithBroker(t, controller, dispatcher)

	// The residual health finding supersedes the parent with a follow-up,
	// stranding the parent's prepared worktree.
	stepAt(t, controller, 3)

	release, ok := controller.PendingReleaseEffect()
	if !ok {
		t.Fatal("superseded parent's prepared worktree was not queued for release")
	}
	if release.IncidentID != first.IncidentID {
		t.Fatalf("release targeted %q, want the superseded parent %q", release.IncidentID, first.IncidentID)
	}
	drainRelease(t, controller)

	// Prepare the follow-up's own worktree; it is live and must never be released.
	followUp, ok := controller.PendingWorkspaceEffect()
	if !ok {
		t.Fatal("follow-up did not prepare its own worktree")
	}
	if followUp.IncidentID == first.IncidentID {
		t.Fatalf("follow-up reused the parent incident id %q", first.IncidentID)
	}
	drainWorkspace(t, controller)

	calls := broker.calls()
	if got := countReleases(calls, first.IncidentID); got != 1 {
		t.Fatalf("parent worktree released %d times, want exactly 1: %v", got, calls)
	}
	if got := countReleases(calls, followUp.IncidentID); got != 0 {
		t.Fatalf("live follow-up worktree was released: %v", calls)
	}
	if _, ok := controller.PendingReleaseEffect(); ok {
		t.Fatal("a release remained pending after the parent was reaped")
	}
}

// TestSupersededReleaseSurvivesRestart proves the pending release is durable:
// a controller that crashes after superseding the parent, before the broker
// reaped it, still reaps it after restoring its persisted state.
func TestSupersededReleaseSurvivesRestart(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 2, healthBPersistent())
	broker := &recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base"}}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	first := completeFirstResponderWithBroker(t, controller, dispatcher)
	stepAt(t, controller, 3)
	if _, ok := controller.PendingReleaseEffect(); !ok {
		t.Fatal("parent worktree was not queued for release before the restart")
	}

	restored, _ := newFollowUpController(t, 2, healthBPersistent())
	if err := restored.RestoreState(controller.ExportState()); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if err := restored.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set restored broker: %v", err)
	}
	release, ok := restored.PendingReleaseEffect()
	if !ok || release.IncidentID != first.IncidentID {
		t.Fatalf("restored controller lost the pending release: got %#v ok=%v", release, ok)
	}
	drainRelease(t, restored)
	if got := countReleases(broker.calls(), first.IncidentID); got != 1 {
		t.Fatalf("restored controller released the parent %d times, want 1: %v", got, broker.calls())
	}
}

// TestFollowUpWithoutBrokerKeepsParentWorktree guards the branch: without a
// broker the follow-up reuses the parent's worktree in place, so there is
// nothing to release and no release must be queued.
func TestFollowUpWithoutBrokerKeepsParentWorktree(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 2, healthBPersistent())
	completeFirstResponder(t, controller, dispatcher)
	stepAt(t, controller, 3)
	if _, ok := controller.PendingDispatchEffect(); !ok {
		t.Fatal("expected the follow-up to reach dispatch without a broker")
	}
	controller.mu.Lock()
	pending := append([]string(nil), controller.pendingReleases...)
	controller.mu.Unlock()
	if len(pending) != 0 {
		t.Fatalf("a brokerless follow-up queued a release: %v", pending)
	}
}
