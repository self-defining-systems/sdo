package runtime

import (
	"context"
	"errors"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

type stageDriftView struct {
	program.ReadView
	diff program.Diff
	err  error
}

func (v stageDriftView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) { return v.diff, v.err }

func TestStageUsesFreshDriftObservationWithoutDiscardingResidue(t *testing.T) {
	for _, tc := range []struct {
		name      string
		diff      program.Diff
		err       error
		remaining int
	}{
		{name: "restored"},
		{name: "residual", diff: program.Diff{Changed: true}, remaining: 1},
		{name: "unavailable", err: errors.New("synthetic read failure"), remaining: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "config"}}
			request := IncidentRequest{IncidentID: "synthetic", Findings: []sdk.Finding{finding}}
			loop := NewResponderLoop(request, nil, staticProvider{}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{}, 2)
			loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
				return stageDriftView{ReadView: program.ReadFrom(v), diff: tc.diff, err: tc.err}
			}
			response := ToolResponse{}
			loop.verify(context.Background(), nil, &response, true)
			state := &IncidentState{Request: request, Findings: request.Findings}
			updateStageFindings(state, response)
			if len(state.Findings) != tc.remaining {
				t.Fatalf("remaining = %v, want %d", state.Findings, tc.remaining)
			}
		})
	}
}

func TestStageRetainsDriftOutsideFreshObservation(t *testing.T) {
	opening := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "opening", Status: sdk.FindingActive}
	newer := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "newer", Status: sdk.FindingActive}
	request := IncidentRequest{IncidentID: "synthetic", Findings: []sdk.Finding{opening}}
	loop := NewResponderLoop(request, nil, staticProvider{}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{}, 2)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return stageDriftView{ReadView: program.ReadFrom(v)} }
	response := ToolResponse{}
	loop.verify(context.Background(), nil, &response, true)
	state := &IncidentState{Request: request, Findings: []sdk.Finding{opening, newer}}
	updateStageFindings(state, response)
	if len(state.Findings) != 1 || state.Findings[0].RuleID != "newer" {
		t.Fatalf("unobserved drift discarded: %v", state.Findings)
	}
}
