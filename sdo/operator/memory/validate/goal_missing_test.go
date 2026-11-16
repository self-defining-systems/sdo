package validate

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// A memory without .sdo/goal.md is not refused (the goal is human-owned, so an
// agent cannot add it); the check names it so the author knows install will
// supply SDO's standard goal.
func TestMissingGoalIsANonBlockingNote(t *testing.T) {
	root := goodTree(t)
	if err := os.Remove(filepath.Join(root, layout.GoalFile)); err != nil {
		t.Fatal(err)
	}
	var notes []Diagnostic
	blocking := diagnose(t, Request{Root: root, Layers: Structural | Semantic, Actor: Upkeep, Phase: InitialPhase, CoverageGaps: &notes})
	for _, d := range blocking {
		if d.Rule == RuleGoalMissing {
			t.Fatalf("a missing goal must not block: %v", d)
		}
	}
	for _, n := range notes {
		if n.Rule == RuleGoalMissing && n.Path == layout.GoalFile {
			return
		}
	}
	t.Fatalf("no %s note in %v", RuleGoalMissing, notes)
}
