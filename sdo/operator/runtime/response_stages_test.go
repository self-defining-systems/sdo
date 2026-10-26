package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
)

type clearingResponseStage struct {
	name  string
	calls *int
}

func (s clearingResponseStage) Name() string { return s.name }
func (s clearingResponseStage) Run(_ context.Context, state *IncidentState) (*contracts.StageReport, error) {
	*s.calls++
	state.Findings = nil
	return &contracts.StageReport{Stage: s.name}, nil
}
func TestResponsePipelineStopsWhenFindingsClear(t *testing.T) {
	first, second := 0, 0
	state := &IncidentState{Findings: []sdk.Finding{{DetectorID: "fault"}}}
	err := RunResponseStages(context.Background(), state, []ResponseStage{clearingResponseStage{"first", &first}, clearingResponseStage{"second", &second}})
	if err != nil || first != 1 || second != 0 || len(state.PriorStages) != 1 {
		t.Fatalf("pipeline: %+v, %v, calls %d/%d", state, err, first, second)
	}
}
