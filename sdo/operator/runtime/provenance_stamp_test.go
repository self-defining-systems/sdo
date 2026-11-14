package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"sdo.dev/operator/provenance"
)

func TestCodeRunningToolResultsNameTheirCommit(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		_, loop := targetLoop(t)
		loop.WorkingProgramRoot = ""
		state := provenance.Repository{Commit: "abc123", Dirty: dirty}
		if dirty {
			state.Snapshot = "def456"
		}
		loop.Request.SourceCommit = "abc123"
		loop.RepositoryState = func(context.Context, string) (provenance.Repository, error) { return state, nil }
		loop.WorkingProgramRoot = t.TempDir()

		response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "aware-restart", Parameters: map[string]string{"deployment": apiBinding}})
		if response.Provenance == nil || response.Provenance.Commit != "abc123" || response.Provenance.Dirty != dirty || response.Provenance.Snapshot != state.Snapshot {
			t.Fatalf("dirty=%v: provenance = %+v", dirty, response.Provenance)
		}
		if response.Provenance.OperatorBuild == "" {
			t.Fatal("operator_build is empty")
		}
		raw, _ := json.Marshal(response)
		var wire struct {
			Provenance provenance.Record `json:"provenance"`
		}
		if err := json.Unmarshal(raw, &wire); err != nil || wire.Provenance.Commit != "abc123" {
			t.Fatalf("provenance did not reach the wire record: %s", raw)
		}
		if len(loop.calls) == 0 {
			t.Fatal("no recorded call")
		}
		var recorded ToolResponse
		if err := json.Unmarshal([]byte(loop.calls[len(loop.calls)-1].ResponseJSON), &recorded); err != nil || recorded.Provenance == nil || recorded.Provenance.Commit != "abc123" {
			t.Fatalf("recorded call lacks the commit: %s", loop.calls[len(loop.calls)-1].ResponseJSON)
		}
	}
}

func TestReadOnlyToolsCarryNoProvenance(t *testing.T) {
	_, loop := targetLoop(t)
	response := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if response.Provenance != nil {
		t.Fatalf("status runs no repository code, got %+v", response.Provenance)
	}
}

func TestStageResultsNameTheAcceptedCommit(t *testing.T) {
	_, loop := targetLoop(t)
	loop.Request.SourceCommit = "acc999"
	loop.WorkingProgramRoot = t.TempDir()
	loop.RepositoryState = func(context.Context, string) (provenance.Repository, error) {
		t.Fatal("the automatic stage must not read a checkout")
		return provenance.Repository{}, nil
	}
	response := ToolResponse{}
	loop.stampProvenance(context.Background(), ToolRequest{Command: "mitigate", stage: true}, &response)
	if response.Provenance == nil || response.Provenance.Commit != "acc999" || response.Provenance.Dirty {
		t.Fatalf("stage provenance = %+v", response.Provenance)
	}
}
