package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/outcome"
)

// With the recording off the agent's own repairs reach SDO only as its reported changes. A success where the stage
// restored one object from source and the agent repaired data itself learns; one where every known or reported change
// is a plain source restore does not. With the recording on, the recording is the record and reports add nothing.
func TestClosingLearnsCountsTheAgentsReportedChanges(t *testing.T) {
	at := timestamppb.New(time.Unix(1000, 0))
	stageRestore := []AppliedPlaybook{{PlaybookID: SourceRestoreID}}
	dataRepair := &contracts.AgentChange{Summary: "copied the documents of the backup collection back", Target: "store collection", At: at}
	reportedRestore := &contracts.AgentChange{Summary: "sdo mitigate " + SourceRestoreID, Target: "ConfigMap app/config", At: at}
	for name, tc := range map[string]struct {
		applied  []AppliedPlaybook
		reported []*contracts.AgentChange
		recorded bool
		want     bool
	}{
		"stage restore and an agent data repair":   {applied: stageRestore, reported: []*contracts.AgentChange{dataRepair}, want: true},
		"stage restore only":                       {applied: stageRestore, want: false},
		"stage restore and a reported restore":     {applied: stageRestore, reported: []*contracts.AgentChange{reportedRestore}, want: false},
		"agent data repair, nothing else known":    {reported: []*contracts.AgentChange{dataRepair}, want: true},
		"recording on: reports do not add changes": {applied: stageRestore, reported: []*contracts.AgentChange{dataRepair}, recorded: true, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			closure := IncidentClosure{Result: &IncidentResult{ResponderSessionID: "session", AppliedPlaybooks: tc.applied, AgentChanges: tc.reported}}
			if got := closingLearnsFor(outcome.Success, closure, &contracts.OutcomeRecord{}, tc.recorded); got != tc.want {
				t.Fatalf("closingLearnsFor = %v, want %v", got, tc.want)
			}
			if closingLearnsFor(outcome.Escalated, closure, &contracts.OutcomeRecord{}, tc.recorded) {
				t.Fatal("an escalated close never learns")
			}
		})
	}
}
