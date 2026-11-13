package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
)

// The closing turn is the one short message the responder's own session gets after Recovered. It resolves the app
// edits the agent left uncaptured (keep or remove) and, when the incident earned it, turns the fix into a playbook.
// Go decides everything about it: whether it runs, what it says about the changes, how many cleanup turns it may
// take and what remains recorded when they are used up.

// closingChangesBudget bounds the change list the learning turn carries, in bytes.
const closingChangesBudget = 1536

// ClosingChange is one recorded change of the incident, ranked for the closing turn.
type ClosingChange struct {
	Line    string
	Restore bool
	rank    int
}

// ClosingChanges renders the incident's recorded changes, one line per change: what reached the cluster (the
// recording's writes) and the playbooks the agent applied with sdo mitigate. Non-restore changes come first, execs and
// mitigations before deletes, restores last. Lines past the budget are counted; a non-restore change is never dropped
// in favour of a restore. Writes to helper objects and lost-record gaps are not changes of the fix.
func ClosingChanges(actions []RepairActionReceipt, mitigated []string) []ClosingChange {
	var changes []ClosingChange
	for _, id := range mitigated {
		change := ClosingChange{Line: "sdo mitigate " + id}
		if id == SourceRestoreID {
			change = ClosingChange{Line: "sdo mitigate " + id + " (source restore: built in, leave out)", Restore: true, rank: 3}
		}
		changes = append(changes, change)
	}
	for _, action := range actions {
		if !action.Success || apihistory.HelperKind(action.Kind) || action.Kind == apihistory.GapKind {
			continue
		}
		verb := strings.TrimPrefix(action.Kind, apihistory.WriteKindPrefix)
		rank := 1
		if verb == "delete" {
			rank = 2
		}
		at := ""
		if !action.CompletedAt.IsZero() {
			at = action.CompletedAt.UTC().Format("15:04:05") + " "
		}
		changes = append(changes, ClosingChange{Line: fmt.Sprintf("%s%s %s", at, verb, action.Target), rank: rank})
	}
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].rank < changes[j].rank })
	return changes
}

// closingChangesOf is the change list of a closure: the recording's writes and the playbooks the agent applied.
func closingChangesOf(closure IncidentClosure) []ClosingChange {
	if closure.Result == nil {
		return nil
	}
	var applied []string
	for _, playbook := range closure.Result.AppliedPlaybooks {
		applied = append(applied, playbook.PlaybookID)
	}
	return ClosingChanges(closure.Result.RepairActions, applied)
}

// closingSkipReason names why a responder session's closing turn does not learn.
func closingSkipReason(classification string) string {
	if classification == outcome.Escalated {
		return "escalated_no_learning"
	}
	return "closing_turn_nothing_to_script"
}

// noLearningDecision is the decision for a closing turn that does not learn. When Go already wrote files into the
// checkout (corpus cases, captured proposals) the decision is "updated", because a no_change over a nonempty diff is
// rejected by the shared check; otherwise it is a no_change that says why.
func noLearningDecision(classification string, wroteFiles bool) *contracts.ReflectionDecision {
	if wroteFiles {
		return &contracts.ReflectionDecision{LearningDecision: "updated"}
	}
	return &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: closingSkipReason(classification)}
}

// ClosingChangeLines is ClosingChanges numbered and cut to the budget. The count of lines left out is the last line.
func ClosingChangeLines(changes []ClosingChange) []string {
	var lines []string
	size, left := 0, 0
	for i, change := range changes {
		line := fmt.Sprintf("%d. %s", i+1, change.Line)
		if size+len(line)+1 > closingChangesBudget && !(size == 0) {
			left = len(changes) - i
			break
		}
		lines = append(lines, line)
		size += len(line) + 1
	}
	if left > 0 {
		lines = append(lines, fmt.Sprintf("+%d more changes not shown", left))
	}
	return lines
}

// closingChangeLines is the learning turn's change list: the numbered recorded changes, or nothing when the
// recording is off (the session knows what it did).
func (n *NativeIncidentBroker) closingChangeLines(closure IncidentClosure) []string {
	if !n.Recording {
		return nil
	}
	return ClosingChangeLines(closingChangesOf(closure))
}

// ClosingLearns reports whether the closing turn may turn the fix into a playbook: the closure is a success, with a
// responder session to ask and at least one change that is not a plain restore from accepted source. An escalated,
// failed, unconfirmed or reset closure teaches nothing; its closing turn only cleans up. With the recording off
// (agent.recording) the agent's own cluster changes are unknown, so a success closure learns unless every change
// SDO does know of is a plain restore; the session is asked and may answer that nothing is worth a playbook.
func ClosingLearns(classification string, changes []ClosingChange, recorded bool) bool {
	if classification != outcome.Success {
		return false
	}
	if !recorded && len(changes) == 0 {
		return true
	}
	for _, change := range changes {
		if !change.Restore {
			return true
		}
	}
	return false
}

// ResolveLeftovers asks the agent, up to limit times, to keep or remove the app edits it left uncaptured. After each
// turn leftovers recomputes what is still uncaptured; the loop ends as soon as nothing is, or after limit turns. What
// remains is returned and the caller records it as source_repair_uncaptured: a failed or unanswered turn never fails
// the closure, and the outcome is committed either way.
func ResolveLeftovers(
	ctx context.Context,
	limit int,
	leftovers func(context.Context) ([]string, error),
	turn func(ctx context.Context, paths []string, number int) error,
) (remaining []string, turns int, err error) {
	remaining, err = leftovers(ctx)
	for err == nil && len(remaining) > 0 && turns < limit {
		turns++
		if turnErr := turn(ctx, remaining, turns); turnErr != nil {
			slog.Warn("closing cleanup turn failed", "turn", turns, "error", turnErr)
			return remaining, turns, nil
		}
		remaining, err = leftovers(ctx)
	}
	return remaining, turns, err
}

// LeftoverCleaner runs one cleanup-only closing turn: the responder's own session, in the proposal checkout, asked to
// keep (capture as a patch) or remove the app files it left uncaptured.
type LeftoverCleaner interface {
	Cleanup(ctx context.Context, closure IncidentClosure, workspace IncidentWorkspace, leftovers []string, turn int) error
}

// cleanupBeforePublish runs the closing turn's cleanup loop after the acknowledgment and before the source
// publication, in the responder's own proposal checkout and outside the ledger lock. It asks the agent, up to
// response.cleanup_turns times, to commit or remove the app files it left uncommitted and to fix a commit the source
// rules would refuse. The turn counter is saved before each dispatch and the end is recorded as the phase
// cleanup_done, so a restart or a deferred publication never repeats a turn. A failed turn never returns an error: it
// ends the loop and publication goes on, because an error here would count toward abandoning committed source.
func (n NativeIncidentBroker) cleanupBeforePublish(ctx context.Context, id string) {
	var ledger *contracts.BrokerLedger
	if err := n.withLock(func() (err error) { ledger, err = n.readLedger(id); return err }); err != nil {
		slog.Warn("closing cleanup skipped", "incident_id", id, "error", err)
		return
	}
	if _, done := ledger.GetClosurePhaseTimestamps()["cleanup_done"]; done || ledger.SourceRepair != sourcePending {
		return
	}
	finish := func() {
		_ = n.withLock(func() error {
			current, err := n.readLedger(id)
			if err != nil {
				return err
			}
			return n.recordPhase(current, "cleanup_done")
		})
	}
	closure := incidentClosureState(ledger.Closure)
	// Escalated closures get no turn (committed source still publishes), nor does a closure without a session,
	// a cleaner or a turn budget.
	if ledger.GetClosure().GetEscalatedAt() != nil || n.Cleaner == nil || n.CleanupTurns <= 0 || !hasResponderSession(closure) {
		finish()
		return
	}
	leftovers := func(ctx context.Context) ([]string, error) {
		scratch, err := n.Repository.UncapturedPaths(ctx, id, broker.ProposalPhase)
		if err != nil {
			return nil, err
		}
		rules, err := n.Repository.SourceRuleDiagnostics(ctx, id, broker.ProposalPhase)
		if err != nil {
			return nil, err
		}
		return append(scratch, rules...), nil
	}
	checkout, err := n.Repository.ProposalCheckout(id, broker.ProposalPhase)
	if err != nil {
		slog.Warn("closing cleanup skipped", "incident_id", id, "error", err)
		finish()
		return
	}
	workspace := IncidentWorkspace{IncidentID: id, Worktree: checkout, BaseCommit: closure.Request.RepositoryBaseCommit}
	used := int(ledger.GetCleanupTurnsUsed())
	_, _, err = ResolveLeftovers(ctx, max(0, n.CleanupTurns-used), leftovers, func(ctx context.Context, lines []string, number int) error {
		turn := used + number
		if err := n.withLock(func() error {
			current, err := n.readLedger(id)
			if err != nil {
				return err
			}
			current.CleanupTurnsUsed = uint32(turn)
			return n.saveLedger(current)
		}); err != nil {
			return err
		}
		return n.Cleaner.Cleanup(ctx, closure, workspace, lines, turn)
	})
	if err != nil {
		slog.Warn("closing cleanup ended on an error", "incident_id", id, "error", err)
	}
	finish()
}
