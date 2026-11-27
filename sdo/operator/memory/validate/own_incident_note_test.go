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
	match := OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentMatch, Capture: "open@detect.json"})
	if match.Rule != RuleOwnIncidentReplay || match.Path != ".sdo/playbooks/fresh" || !strings.HasPrefix(match.Message, "match:") {
		t.Fatalf("match note = %+v", match)
	}
	blind := OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "fresh", Outcome: sdktest.OwnIncidentUnverified, Reason: "no captured open state"})
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

// Go reads back which capture a new playbook's Diagnose matched in, so its
// first corpus case comes from that capture; other notes say nothing.
func TestOwnIncidentMatchCaptureReadsTheNoteBack(t *testing.T) {
	note := OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "later", Outcome: sdktest.OwnIncidentMatch, Capture: "open@join-2.json"})
	if id, phase, ok := OwnIncidentMatchCapture(note.Rule, note.Path, note.Message); !ok || id != "later" || phase != "join-2" {
		t.Fatalf("read back = %q %q %v", id, phase, ok)
	}
	blind := OwnIncidentNote(sdktest.OwnIncidentResult{ProgramID: "later", Outcome: sdktest.OwnIncidentUnverified, Reason: "no captured open state"})
	if _, _, ok := OwnIncidentMatchCapture(blind.Rule, blind.Path, blind.Message); ok {
		t.Fatal("an unverified note named a capture")
	}
}
