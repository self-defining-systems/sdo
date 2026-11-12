//go:build integration

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// recordedCleaner stands in for the responder's session: it records each
// turn's leftover lines and acts on the proposal checkout.
type recordedCleaner struct {
	turns   atomic.Int32
	lines   [][]string
	act     func(turn int, checkout string)
	failing bool
}

func (c *recordedCleaner) Cleanup(_ context.Context, _ IncidentClosure, workspace IncidentWorkspace, leftovers []string, turn int) error {
	c.turns.Add(1)
	c.lines = append(c.lines, append([]string(nil), leftovers...))
	if c.act != nil {
		c.act(turn, workspace.Worktree)
	}
	if c.failing {
		return errors.New("the agent did not answer")
	}
	return nil
}

func sessionClosure(w *repairClosureWorld, policy string) IncidentClosure {
	closure := w.closure(policy)
	closure.Result.ResponderSessionID = "session-1"
	return closure
}

func closeWithSession(t *testing.T, w *repairClosureWorld) error {
	t.Helper()
	ctx := context.Background()
	receipt, err := w.native.ProcessClosure(ctx, sessionClosure(w, "recorded-actions"))
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	if err := w.native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	return w.native.ProcessQueuedReflection(ctx, "incident-1")
}

// Scratch left after the first turn gets a second prompt; a commit ends the
// loop and the committed file is what gets published.
func TestTheClosingTurnAsksAgainUntilTheScratchIsResolved(t *testing.T) {
	repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
	w := newRepairClosureWorld(t, repairs, "scratch")
	repairs.publish = w.publishLikeTheDeployedPath
	cleaner := &recordedCleaner{act: func(turn int, checkout string) {
		if turn == 2 {
			repairGit(t, checkout, "config", "user.name", "Synthetic Agent")
			repairGit(t, checkout, "config", "user.email", "agent@example.invalid")
			repairGit(t, checkout, "add", "app.txt")
			repairGit(t, checkout, "commit", "-qm", "keep the edit")
		}
	}}
	w.native.Cleaner, w.native.CleanupTurns = cleaner, 2
	if err := closeWithSession(t, w); err != nil {
		t.Fatal(err)
	}
	if cleaner.turns.Load() != 2 || len(cleaner.lines[0]) != 1 || cleaner.lines[0][0] != "app.txt" {
		t.Fatalf("turns %d, lines %v: scratch left after one turn must be asked about again", cleaner.turns.Load(), cleaner.lines)
	}
	ledger, _ := w.native.readLedger("incident-1")
	if ledger.GetSourceRepair() != "deployed" || ledger.GetCleanupTurnsUsed() != 2 {
		t.Fatalf("source repair %q, turns used %d", ledger.GetSourceRepair(), ledger.GetCleanupTurnsUsed())
	}
}

// After the budget the scratch is discarded and recorded; a failing turn does
// not fail or abandon the publication, and a later dispatch never asks again.
func TestAFailingOrUnansweredTurnEndsInADiscardAndNeverRepeats(t *testing.T) {
	repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
	w := newRepairClosureWorld(t, repairs, "scratch")
	cleaner := &recordedCleaner{failing: true}
	w.native.Cleaner, w.native.CleanupTurns = cleaner, 2
	if err := closeWithSession(t, w); err != nil {
		t.Fatalf("a failed cleanup turn must not fail the publication: %v", err)
	}
	if cleaner.turns.Load() != 1 {
		t.Fatalf("a failed turn ends the loop: %d turns", cleaner.turns.Load())
	}
	ledger, _ := w.native.readLedger("incident-1")
	if ledger.GetSourceRepair() != "none" || len(ledger.GetSourceUncaptured()) != 1 {
		t.Fatalf("source repair %q, uncaptured %v", ledger.GetSourceRepair(), ledger.GetSourceUncaptured())
	}
	if _, done := ledger.GetClosurePhaseTimestamps()["cleanup_done"]; !done {
		t.Fatal("cleanup_done was not recorded")
	}
}

// A closure that escalated gets no cleanup turn, and without a session none is asked.
func TestNoCleanupTurnWithoutASessionOrForAnEscalatedClosure(t *testing.T) {
	for name, tc := range map[string]func(*repairClosureWorld, *IncidentClosure){
		"escalated":  func(_ *repairClosureWorld, c *IncidentClosure) { c.EscalatedAt = c.VerifiedAt.Add(1) },
		"no session": func(_ *repairClosureWorld, c *IncidentClosure) { c.Result.ResponderSessionID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
			w := newRepairClosureWorld(t, repairs, "scratch")
			cleaner := &recordedCleaner{}
			w.native.Cleaner, w.native.CleanupTurns = cleaner, 2
			ctx := context.Background()
			closure := sessionClosure(w, "recorded-actions")
			tc(w, &closure)
			receipt, err := w.native.ProcessClosure(ctx, closure)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.native.AcknowledgeClosure(ctx, receipt); err != nil {
				t.Fatal(err)
			}
			if err := w.native.ProcessQueuedReflection(ctx, "incident-1"); err != nil {
				t.Fatal(err)
			}
			if cleaner.turns.Load() != 0 {
				t.Fatalf("%d cleanup turns asked", cleaner.turns.Load())
			}
		})
	}
}

// The turn also hears about a commit the source rules would refuse.
func TestTheCleanupTurnReportsACommitTheSourceRulesWouldRefuse(t *testing.T) {
	repairs := &recordedRepairer{result: SourceRepair{Deployed: true}}
	w := newRepairClosureWorld(t, repairs, "committed")
	checkout := w.workspace.Worktree
	if err := os.WriteFile(filepath.Join(checkout, "secret.yaml"), []byte("kind: Secret\nstringData:\n  password: hunter2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repairGit(t, checkout, "add", "secret.yaml")
	repairGit(t, checkout, "commit", "-qm", "add the credential")
	cleaner := &recordedCleaner{}
	w.native.Cleaner, w.native.CleanupTurns = cleaner, 1
	_ = closeWithSession(t, w)
	if cleaner.turns.Load() != 1 || !strings.Contains(strings.Join(cleaner.lines[0], "\n"), "secret.yaml") {
		t.Fatalf("turns %d, lines %v: the refused commit must be named", cleaner.turns.Load(), cleaner.lines)
	}
}
