package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/contracts/outcome"
)

func TestClosingChangesRankNonRestoresFirstAndCountWhatIsCut(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	actions := []RepairActionReceipt{
		{Kind: "api-delete", Target: "Pod/api-1", Success: true, CompletedAt: at},
		{Kind: "api-exec", Target: "Pod/db-1", Success: true, CompletedAt: at},
		{Kind: "api-helper-create", Target: "Pod/scratch", Success: true},
		{Kind: "api-gap", Target: "api-history/gap", Success: true},
	}
	changes := ClosingChanges(actions, []string{SourceRestoreID, "fix-db"})
	lines := ClosingChangeLines(changes)
	got := strings.Join(lines, "\n")
	for _, want := range []string{"1. sdo mitigate fix-db", "2. 03:04:05 exec Pod/db-1", "3. 03:04:05 delete Pod/api-1", "4. sdo mitigate source-restore (source restore: built in, leave out)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("change list missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "scratch") || strings.Contains(got, "gap") {
		t.Fatalf("helper writes and gaps are not changes of the fix:\n%s", got)
	}

	var many []ClosingChange
	for i := 0; i < 200; i++ {
		many = append(many, ClosingChange{Line: strings.Repeat("x", 40)})
	}
	cut := ClosingChangeLines(many)
	if size := len(strings.Join(cut, "\n")); size > closingChangesBudget+64 {
		t.Fatalf("change list is %d bytes, budget %d", size, closingChangesBudget)
	}
	if !strings.HasPrefix(cut[len(cut)-1], "+") || !strings.HasSuffix(cut[len(cut)-1], "more changes not shown") {
		t.Fatalf("cut lines must be counted, got %q", cut[len(cut)-1])
	}
}

func TestClosingLearnsOnlyFromASuccessWithANonRestoreChange(t *testing.T) {
	fix := []ClosingChange{{Line: "exec"}}
	restore := []ClosingChange{{Line: "restore", Restore: true}}
	for name, tc := range map[string]struct {
		class   string
		changes []ClosingChange
		want    bool
	}{
		"success with a fix":    {outcome.Success, fix, true},
		"success restore only":  {outcome.Success, restore, false},
		"success no changes":    {outcome.Success, nil, false},
		"escalated close":       {outcome.Escalated, fix, false},
		"failed close":          {outcome.Failed, fix, false},
		"unconfirmed close":     {outcome.Unconfirmed, fix, false},
		"cleared without agent": {outcome.ClearedWithoutAction, fix, false},
	} {
		if got := ClosingLearns(tc.class, tc.changes); got != tc.want {
			t.Errorf("%s: ClosingLearns = %v, want %v", name, got, tc.want)
		}
	}
}

func TestResolveLeftoversEndsAfterTheCleanupTurnLimit(t *testing.T) {
	left := []string{"app/main.go"}
	var asked []int
	remaining, turns, err := ResolveLeftovers(context.Background(), 2,
		func(context.Context) ([]string, error) { return left, nil },
		func(_ context.Context, paths []string, number int) error {
			asked = append(asked, number)
			return nil // the agent answers but never cleans up
		})
	if err != nil || turns != 2 || len(asked) != 2 || len(remaining) != 1 {
		t.Fatalf("turns=%d asked=%v remaining=%v err=%v, want exactly 2 turns and the leftover still recorded", turns, asked, remaining, err)
	}
}

func TestResolveLeftoversStopsWhenCleanAndAsksNothingAtZero(t *testing.T) {
	left := []string{"a", "b"}
	remaining, turns, _ := ResolveLeftovers(context.Background(), 5,
		func(context.Context) ([]string, error) { return left, nil },
		func(context.Context, []string, int) error { left = nil; return nil })
	if turns != 1 || len(remaining) != 0 {
		t.Fatalf("a cleaned checkout ends the loop after the turn that cleaned it: turns=%d remaining=%v", turns, remaining)
	}
	_, turns, _ = ResolveLeftovers(context.Background(), 0,
		func(context.Context) ([]string, error) { return []string{"a"}, nil },
		func(context.Context, []string, int) error { t.Fatal("cleanup_turns 0 asks no turn"); return nil })
	if turns != 0 {
		t.Fatalf("turns = %d, want 0", turns)
	}
}

func TestResolveLeftoversTurnFailureNeverFailsTheClosure(t *testing.T) {
	remaining, turns, err := ResolveLeftovers(context.Background(), 3,
		func(context.Context) ([]string, error) { return []string{"a"}, nil },
		func(context.Context, []string, int) error { return errors.New("worker killed") })
	if err != nil || turns != 1 || len(remaining) != 1 {
		t.Fatalf("a failed turn is not retried and not an error: turns=%d remaining=%v err=%v", turns, remaining, err)
	}
}

func TestAnEscalatedCloseGetsCleanupOnlyAndSaysWhy(t *testing.T) {
	closure := IncidentClosure{Result: &IncidentResult{
		ResponderSessionID: "session-1",
		AppliedPlaybooks:   []AppliedPlaybook{{PlaybookID: "fix-db"}},
		RepairActions:      []RepairActionReceipt{{Kind: "api-exec", Target: "Pod/db-1", Success: true}},
	}}
	changes := closingChangesOf(closure)
	if len(changes) != 2 || changes[0].Line != "sdo mitigate fix-db" {
		t.Fatalf("changes = %+v, want the applied playbook then the recorded exec", changes)
	}
	if !ClosingLearns(outcome.Success, changes) {
		t.Fatal("a success with a fix must learn")
	}
	if ClosingLearns(outcome.Escalated, changes) {
		t.Fatal("an escalated close must not learn, whatever it changed")
	}
	if got := closingSkipReason(outcome.Escalated); got != "escalated_no_learning" {
		t.Fatalf("reason = %q, want escalated_no_learning", got)
	}
	if got := closingSkipReason(outcome.Failed); got != "closing_turn_nothing_to_script" {
		t.Fatalf("reason = %q", got)
	}
	if got := closingChangesOf(IncidentClosure{}); got != nil {
		t.Fatalf("a closure without a result has no changes, got %+v", got)
	}
}

// An escalated close gets no learning turn on either code path, and files Go already wrote make the decision an update
// (a no_change over a nonempty diff is rejected by the shared check).
func TestNoLearningDecisionKeepsFilesGoWrote(t *testing.T) {
	for _, class := range []string{outcome.Escalated, outcome.Failed, outcome.Unconfirmed} {
		if ClosingLearns(class, []ClosingChange{{Line: "exec"}}) {
			t.Errorf("%s close must not learn", class)
		}
	}
	none := noLearningDecision(outcome.Escalated, false)
	if none.LearningDecision != "no_change" || none.NoChangeReason != "escalated_no_learning" {
		t.Fatalf("decision = %+v", none)
	}
	wrote := noLearningDecision(outcome.Escalated, true)
	if wrote.LearningDecision != "updated" || wrote.NoChangeReason != "" {
		t.Fatalf("decision with Go-written files = %+v, want updated", wrote)
	}
}
