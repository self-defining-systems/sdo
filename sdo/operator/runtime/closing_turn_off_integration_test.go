//go:build integration

package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// closeUnder closes a verified success closure that has a responder session, acknowledges it and runs whatever the
// controller would run after the acknowledgment, under the given closing-turn policy. It returns the broker ledger,
// the reflection turns and the cleanup turns the session was asked, and whether the incident was still queued after
// the acknowledgment (what the controller state projects and a benchmark harness waits on). leaveScratch leaves an
// uncommitted app edit in the checkout, which the cleanup turn asks about.
func closeUnder(t *testing.T, closingTurnOff, leaveScratch bool) (*contracts.BrokerLedger, int32, int32, bool) {
	t.Helper()
	w := newRepairClosureWorld(t, &recordedRepairer{result: SourceRepair{Deployed: true}}, "scratch")
	if !leaveScratch {
		// A cluster-only repair: the checkout is left as it was prepared, so no source publication is owed.
		repairGit(t, w.workspace.Worktree, "checkout", "--", "app.txt")
	}
	var reflections atomic.Int32
	w.native.Reflector = ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		reflections.Add(1)
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic"}, nil
	})
	cleaner := &recordedCleaner{}
	w.native.Cleaner, w.native.CleanupTurns = cleaner, 2
	w.native.ClosingTurnOff = closingTurnOff
	ctx := context.Background()
	receipt, err := w.native.ProcessClosure(ctx, sessionClosure(w, "recorded-actions"))
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	if err := w.native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	queued, err := w.native.QueuedReflections()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.native.ProcessQueuedReflection(ctx, "incident-1"); err != nil {
		t.Fatal(err)
	}
	ledger, err := w.native.readLedger("incident-1")
	if err != nil {
		t.Fatal(err)
	}
	return ledger, reflections.Load(), cleaner.turns.Load(), slices.Contains(queued, "incident-1")
}

// With learning.closing_turn off a verified closure commits the same outcome a learning closure commits, and its
// work ends at the acknowledgment: no closing turn and nothing queued behind it.
func TestClosingTurnOffEndsTheIncidentAtTheAcknowledgedOutcome(t *testing.T) {
	learning, _, _, learningQueued := closeUnder(t, false, false)
	if !learning.GetRequiresReflection() || !learningQueued {
		t.Fatalf("closing turn on: requires reflection %v, queued %v; want the closing turn queued", learning.GetRequiresReflection(), learningQueued)
	}

	off, turns, _, queued := closeUnder(t, true, false)
	if turns != 0 {
		t.Fatalf("closing turn off: the session was asked %d closing turns, want none", turns)
	}
	if off.GetRequiresReflection() || queued {
		t.Fatalf("closing turn off: requires reflection %v, queued after the acknowledgment %v; want the incident's work done", off.GetRequiresReflection(), queued)
	}
	if !off.GetAcknowledged() || off.GetOutcomeCommit() == "" {
		t.Fatalf("closing turn off: acknowledged %v, outcome commit %q; want the outcome committed and acknowledged", off.GetAcknowledged(), off.GetOutcomeCommit())
	}
	if off.GetReflectionSkippedReason() != ClosingTurnOffReason {
		t.Fatalf("reflection_skipped_reason = %q, want %q", off.GetReflectionSkippedReason(), ClosingTurnOffReason)
	}
	if !proto.Equal(worldFree(off.GetOutcomeDocument()), worldFree(learning.GetOutcomeDocument())) {
		t.Fatalf("closing turn off changed the committed outcome:\n off %v\n on  %v", off.GetOutcomeDocument(), learning.GetOutcomeDocument())
	}
}

// With the closing turn off the session is not asked about app edits it left either: they are discarded and
// recorded as uncaptured, as after a cleanup budget of zero.
func TestClosingTurnOffAsksNoCleanupTurn(t *testing.T) {
	if _, _, cleanups, _ := closeUnder(t, false, true); cleanups == 0 {
		t.Fatal("closing turn on: the session was asked no cleanup turn about its scratch edit")
	}
	off, turns, cleanups, _ := closeUnder(t, true, true)
	if turns != 0 || cleanups != 0 {
		t.Fatalf("closing turn off: %d closing and %d cleanup turns, want none", turns, cleanups)
	}
	if len(off.GetSourceUncaptured()) != 1 {
		t.Fatalf("uncaptured %v, want the scratch edit recorded", off.GetSourceUncaptured())
	}
}

// worldFree is an outcome without what differs between two synthetic worlds: their repository commits and clock.
func worldFree(record *contracts.OutcomeRecord) *contracts.OutcomeRecord {
	record = proto.Clone(record).(*contracts.OutcomeRecord)
	record.SourceCommit, record.DeployedCommit = "", ""
	var clear func(protoreflect.Message)
	clear = func(message protoreflect.Message) {
		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.Message() == nil || field.IsMap() {
				return true
			}
			if field.Message().FullName() == "google.protobuf.Timestamp" {
				message.Clear(field)
				return true
			}
			if field.IsList() {
				for i := 0; i < value.List().Len(); i++ {
					clear(value.List().Get(i).Message())
				}
				return true
			}
			clear(value.Message())
			return true
		})
	}
	clear(record.ProtoReflect())
	return record
}
