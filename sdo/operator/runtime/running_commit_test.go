package runtime

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/provenance"
	"sdo.dev/operator/sdk"
)

func TestChangesNameTheCommitAnAgentWriteRanOn(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{
		{At: onset, Source: "write", ReaderOrObject: "deployment/api", Key: "patch", Writer: sdk.ResponderFieldManager, Actor: ActorAgent, RunningCommit: "abc123"},
		{At: onset, Source: "write", ReaderOrObject: "deployment/api", Key: "mitigation", Writer: "sdo mitigate", Actor: ActorAgent, RunningCommit: "abc123", RunningDirty: true},
		{At: onset, Source: "kubernetes", ReaderOrObject: "ReplicaSet/api", Key: "rollout", Writer: "deployment controller", Actor: ActorKubernetes},
	}
	table := parseCSV(t, renderChanges(rows, onset))
	if got := strings.Join([]string{table[1][10], table[2][10], table[3][10]}, "|"); got != "abc123|abc123+dirty|" {
		t.Fatalf("running_commit column = %q", got)
	}
}

func TestMitigateApplicationsRecordTheCheckoutAtWriteTime(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		_, loop := targetLoop(t)
		loop.WorkingProgramRoot = t.TempDir()
		loop.RepositoryState = func(context.Context, string) (provenance.Repository, error) {
			return provenance.Repository{Commit: "feed42", Dirty: dirty}, nil
		}
		response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Targets: []MitigateTarget{{Name: "aware-restart", Parameters: map[string]BindValues{"deployment": {apiBinding}}}}})
		if response.Error != "" {
			t.Fatalf("mitigate failed: %s", response.Error)
		}
		var rows []IncidentChange
		for _, row := range loop.incidentWrites() {
			if row.Actor == ActorAgent && row.Key == "mitigation" {
				rows = append(rows, row)
			}
		}
		if len(rows) == 0 {
			t.Fatal("no mitigation row in the changes list")
		}
		for _, row := range rows {
			if row.RunningCommit != "feed42" || row.RunningDirty != dirty {
				t.Fatalf("dirty=%v: row = %+v", dirty, row)
			}
		}
	}
}

func TestStageApplicationsNameTheAcceptedCommit(t *testing.T) {
	_, loop := targetLoop(t)
	loop.Request.SourceCommit = "acc999"
	loop.WorkingProgramRoot = t.TempDir()
	loop.RepositoryState = func(context.Context, string) (provenance.Repository, error) {
		t.Fatal("the automatic stage must not read a checkout")
		return provenance.Repository{}, nil
	}
	commit, dirty, _ := loop.checkoutState(context.Background(), true, "apply")
	if commit != "acc999" || dirty {
		t.Fatalf("stage checkout = %q dirty=%v", commit, dirty)
	}
}
