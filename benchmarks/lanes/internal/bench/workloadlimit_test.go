package bench

import (
	"testing"
	"time"
)

// `run active` and `run undo` share the verb `run` with a run, whose limit is six hours: a stuck undo must not hold the lane
// that long.
func TestRunActiveAndUndoAreNotBoundedLikeARun(t *testing.T) {
	if got := workloadLimit(VerbRun, "run"); got != 6*time.Hour {
		t.Errorf("a run: %s", got)
	}
	for _, action := range []string{"active", "undo"} {
		if got := workloadLimit(VerbRun, action); got > time.Hour {
			t.Errorf("run %s is bounded at %s", action, got)
		}
	}
	if got := workloadLimit(VerbGate, "check"); got != hookTimeouts[VerbGate] {
		t.Errorf("gate: %s", got)
	}
}
