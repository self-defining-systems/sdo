package deployment

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/sdk"
)

// The health probe decodes what the controller binary encodes with the same
// evaluation.Verdict type; this pins that every status and each kind of detail
// survives the round trip into the deployer's feedback.
func TestHealthProbeDecodesEveryVerdictTheRuntimeEncodes(t *testing.T) {
	cases := []struct {
		name     string
		verdict  evaluation.Verdict
		status   string
		detector string
		state    string
		reason   string
	}{
		{name: "clear", verdict: evaluation.Verdict{Status: evaluation.Clear, Findings: []sdk.Finding{}}, status: "clear"},
		{name: "pending", verdict: evaluation.Verdict{Status: evaluation.Pending, DetectorStates: []evaluation.DetectorState{{DetectorID: "traffic", Status: "pending", Reason: "window incomplete"}}}, status: "pending", detector: "traffic", state: "pending", reason: "window incomplete"},
		{name: "unavailable", verdict: evaluation.Verdict{Status: evaluation.Unavailable, Unavailable: []evaluation.UnavailableDetector{{DetectorID: "traffic-web", Reason: "prober down"}}}, status: "unavailable", detector: "traffic-web", state: "unavailable", reason: "prober down"},
		{name: "firing", verdict: evaluation.Verdict{Status: evaluation.Firing, Findings: []sdk.Finding{{DetectorID: "web", RuleID: "r", Status: sdk.FindingActive, Summary: "web down"}}}, status: "firing", detector: "web", state: "firing", reason: "web down"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdout, err := json.Marshal(tc.verdict)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeHealthVerdict(stdout, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.status {
				t.Fatalf("status = %q, want %q", got.Status, tc.status)
			}
			if tc.detector == "" {
				if len(got.Detectors) != 0 {
					t.Fatalf("unexpected detectors %+v", got.Detectors)
				}
				return
			}
			if len(got.Detectors) != 1 || got.Detectors[0].DetectorID != tc.detector || got.Detectors[0].Status != tc.state || got.Detectors[0].Reason != tc.reason {
				t.Fatalf("detectors = %+v", got.Detectors)
			}
		})
	}
}

// A detector this environment cannot read that is not a user test (resource
// usage without CFS counters, a clue) is a coverage note on a clear verdict,
// never a detector the verification waits on: a head or a repair is accepted.
func TestHealthProbeRecordsAnUnavailableNonUserTestAsACoverageNote(t *testing.T) {
	reason := "resource usage: CFS counters unavailable for store-0/store"
	stdout, err := json.Marshal(evaluation.Verdict{Status: evaluation.Clear, Findings: []sdk.Finding{}, Unavailable: []evaluation.UnavailableDetector{{DetectorID: "resource-saturation", Reason: reason}, {DetectorID: "cpu-throttling", Reason: reason}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeHealthVerdict(stdout, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Detectors) != 0 {
		t.Fatalf("detectors = %+v, want none: an unavailable non-user-test detector holds nothing", got.Detectors)
	}
	if len(got.CoverageNotes) != 2 || got.CoverageNotes[0].Status != "unavailable" || got.CoverageNotes[0].Reason != reason {
		t.Fatalf("coverage notes = %+v, want both detectors recorded unavailable with their reason", got.CoverageNotes)
	}
	if verdict := verdictOf(got); verdict.Outcome != lifecycle.VerificationClear {
		t.Fatalf("outcome = %v, want clear", verdict.Outcome)
	}
}

func TestHealthProbeRejectsVerdictFieldsTheContractDoesNotDefine(t *testing.T) {
	if _, err := decodeHealthVerdict([]byte(`{"status":"clear","renamed_states":[]}`), nil); err == nil {
		t.Fatal("unknown verdict field accepted; a rename on one side would silently drop detail")
	}
	if _, err := decodeHealthVerdict([]byte(`{"status":"maybe"}`), nil); err == nil {
		t.Fatal("unknown status accepted")
	}
}
