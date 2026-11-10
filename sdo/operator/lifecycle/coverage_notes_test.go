package lifecycle

import (
	"strings"
	"testing"

	"sdo.dev/operator/operability"
)

// A clear verdict whose detectors noted what they could not observe accepts,
// and the report records each gap as an O3 coverage note: visible in the
// persisted operability report, never a gate.
func TestClearVerdictRecordsCoverageNotesWithoutHoldingAcceptance(t *testing.T) {
	verdict := HealthVerdict{
		Outcome: VerificationClear, Summary: "accepted health detectors report clear",
		CoverageNotes: []DetectorState{{DetectorID: "store-state-kv", Status: "clear", Reason: "store kv (redis) not observed until it is read healthy once: Pod kv-0: sh: redis-cli: not found"}},
	}
	checks := verdictChecks(verdict)
	var note *operability.Check
	for i := range checks {
		if strings.HasPrefix(checks[i].ID, "O3.") {
			note = &checks[i]
		}
	}
	if note == nil || note.Status != operability.StatusNote || !strings.Contains(note.Detail, "store-state-kv") || !strings.Contains(note.Detail, "redis-cli") {
		t.Fatalf("checks = %+v", checks)
	}
	subject := operability.Subject{AcceptedCommit: "c", BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p", RenderDigest: "r", EnvDigest: "v", DiagnosticsTree: "d"}
	pass := func(id string) operability.Check {
		return operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass}
	}
	report, err := operability.New(operability.StageComplete, subject).With(append(checks, pass("O1.ready"), pass("O2.images"), pass("O5.memory"), pass("O6.scope"))...)
	if err != nil {
		t.Fatal(err)
	}
	if decision := report.Decide(operability.StageItems(operability.StageComplete)...); decision.Kind != operability.Accepted {
		t.Fatalf("a coverage note held acceptance: %+v", decision)
	}
	if strings.Contains(verdict.Feedback(), "store-state-kv") {
		t.Fatalf("a coverage note is worded as a detector state: %q", verdict.Feedback())
	}
	// A pending or failing verdict carries its notes too.
	verdict.Outcome = VerificationPending
	if !hasCheck(verdictChecks(verdict), "O3.detector-coverage") {
		t.Fatal("a pending verdict dropped its coverage notes")
	}
}

func hasCheck(checks []operability.Check, id string) bool {
	for _, check := range checks {
		if check.ID == id {
			return true
		}
	}
	return false
}
