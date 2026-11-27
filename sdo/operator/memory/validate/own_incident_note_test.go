package validate

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestOwnIncidentFailureLinesBecomeDiagnosticsOnThePlaybook(t *testing.T) {
	output := "--- FAIL: TestNewProgramsDiagnoseTheirOwnIncident\n    own_incident_test.go:9: playbook.own-incident-replay[fresh]: Diagnose returned NoMatch on the incident it was written for: x\nFAIL\n"
	matches := ownIncidentViolation.FindAllStringSubmatch(output, -1)
	if len(matches) != 1 || matches[0][1] != "fresh" || !strings.HasPrefix(matches[0][2], "Diagnose returned NoMatch") {
		t.Fatalf("matches = %q", matches)
	}
}

func TestOwnIncidentNotesNameTheOutcomeAndThePlaybook(t *testing.T) {
	match := ownIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentMatch, Capture: "open@detect.json"})
	if match.Rule != RuleOwnIncidentReplay || match.Path != ".sdo/playbooks/fresh" || !strings.HasPrefix(match.Message, "match:") {
		t.Fatalf("match note = %+v", match)
	}
	blind := ownIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentUnverified, Reason: "no captured open state"})
	if blind.Message != "unverified: no captured open state" {
		t.Fatalf("unverified note = %+v", blind)
	}
}

func TestRecoveredMatchNoteBoundsObjectsAndText(t *testing.T) {
	refs := []sdk.ObjectRef{{Kind: "Pod", Namespace: "app", Name: strings.Repeat("z", 121)}}
	for i := range 15 {
		refs = append(refs, sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: strings.Repeat("a", 110) + string(rune('a'+i))})
	}
	result := sdktest.OwnIncidentResult{ProgramID: "fresh", Recovered: &sdktest.RecoveredMatch{Objects: refs}}
	note, ok := recoveredMatchNote(result)
	if !ok || len(note.Message) >= 1500 || strings.Count(note.Message, "Pod/app/") > 12 ||
		strings.Contains(note.Message, "zz") || !strings.HasPrefix(note.Message, "also Matches after recovery: Pod/app/") ||
		!strings.HasSuffix(note.Message, "a standing condition may truly match: then say so in the README") {
		t.Fatalf("recovered note (%d bytes) = %+v", len(note.Message), note)
	}
}

// The broker reads back which capture a new playbook matched from its note.
func TestOwnIncidentMatchCaptureReadsTheNote(t *testing.T) {
	note := ownIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentMatch, Capture: "open@late-2.json"})
	if got, ok := OwnIncidentMatchCapture(note.Rule, note.Message); !ok || got != "open@late-2.json" {
		t.Fatalf("capture = %q, %v from %q", got, ok, note.Message)
	}
	unverified := ownIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentUnverified, Capture: "open@late-2.json", Reason: "x (y)"})
	if _, ok := OwnIncidentMatchCapture(unverified.Rule, unverified.Message); ok {
		t.Fatal("an unverified note names no matched capture")
	}
}
