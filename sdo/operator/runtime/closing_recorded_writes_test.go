package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
)

// The agent's own Kubernetes writes reach the outcome from the recording, not the worker's result, so the closing
// turn's change list reads the outcome's repair actions: a success whose only fix was a recorded patch must learn.
func TestClosingChangesIncludeTheOutcomesRecordedWrites(t *testing.T) {
	closure := IncidentClosure{Result: &IncidentResult{ResponderSessionID: "session-1"}}
	recorded := &contracts.OutcomeRecord{RepairActions: []*contracts.RepairActionReceipt{
		{Kind: "api-patch", Target: "Service/frontend", Success: true},
	}}
	changes := closingChangesOf(closureWithRecordedActions(closure, recorded))
	if len(changes) != 1 {
		t.Fatalf("changes = %+v, want the recorded patch", changes)
	}
	if !ClosingLearns(outcome.Success, changes, true) {
		t.Fatal("a success whose fix was a recorded patch must learn")
	}
	if len(closure.Result.RepairActions) != 0 {
		t.Fatal("the closure must not be modified")
	}
}
