package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
)

func TestRECEmptyControllerSnapshotsSurviveWire(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after []sdk.Finding
	}{
		{"empty", []sdk.Finding{}, []sdk.Finding{}},
		{"absent", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(ToolResponse{BeforeFindings: tc.before, ControllerFindings: tc.after})
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"before_findings", "controller_findings"} {
				value, ok := wire[key]
				if !ok {
					t.Errorf("%s omitted: %s", key, data)
					continue
				}
				want := "[]"
				if tc.before == nil {
					want = "null"
				}
				if string(value) != want {
					t.Errorf("%s=%s, want %s", key, value, want)
				}
			}
		})
	}
}

func TestRECMutationCachedSnapshotTransport(t *testing.T) {
	finding := sdk.Finding{DetectorID: "route", RuleID: "unreachable", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Status: sdk.FindingActive, Evidence: "connection refused"}
	for _, tc := range []struct {
		name                 string
		before, after        []sdk.Finding
		newCount, fixedCount int
	}{
		{"empty-before", []sdk.Finding{}, []sdk.Finding{finding}, 1, 0},
		{"empty-after", []sdk.Finding{finding}, []sdk.Finding{}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loop := NewResponderLoop(IncidentRequest{IncidentID: "snapshot-wire", Namespace: "app"}, nil, nil, nil, PlanExecutor{}, 5)
			calls := 0
			loop.CachedFindings = func() ToolResponse {
				calls++
				if calls == 1 {
					return ToolResponse{Findings: tc.before}
				}
				return ToolResponse{Findings: tc.after}
			}
			response := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"unknown"}})
			if response.Error == "" {
				t.Fatal("expected unknown plan rejection")
			}
			if response.Progress == nil || len(response.Progress.New) != tc.newCount || len(response.Progress.Fixed) != tc.fixedCount || len(response.Progress.Unchanged) != 0 {
				t.Fatalf("incorrect cached controller progress: %+v", response.Progress)
			}
			data, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ToolResponse
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.BeforeFindings == nil || decoded.ControllerFindings == nil {
				t.Fatalf("observed empty snapshot lost on wire: %s", data)
			}
		})
	}
}

func TestRECRollbackSnapshotTransport(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "rollback-snapshots", Namespace: "app"}, nil, nil, nil, PlanExecutor{}, 5)
	calls := 0
	loop.CachedFindings = func() ToolResponse {
		calls++
		if calls == 1 {
			return ToolResponse{Findings: []sdk.Finding{}}
		}
		return ToolResponse{Findings: []sdk.Finding{{Fingerprint: "new", DetectorID: "health"}}}
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "rollback", ApplicationID: "unknown"})
	if response.Error == "" {
		t.Fatal("expected unknown application rejection")
	}
	if response.Progress == nil || len(response.Progress.New) != 1 || response.BeforeFindings == nil {
		t.Fatalf("rollback lost authoritative snapshots: %+v", response)
	}
}
