package validate

import (
	"strings"
	"testing"

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
