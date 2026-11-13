package evidencesink

import "testing"

func TestInvestigatorReportsAreRunEvidence(t *testing.T) {
	place, ok := Place("sessions/abc/investigations/page-1-inv-1.txt")
	if !ok || place != "incidents/abc/investigations/page-1-inv-1.txt" {
		t.Fatalf("report placed at %q, %v", place, ok)
	}
	if _, ok := Place("sessions/abc/investigations/page-1-inv-1.sh"); ok {
		t.Fatal("only .txt reports are evidence")
	}
}
