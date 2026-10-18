package runtime

import (
	"testing"

	"sdo.dev/operator/sdk"
)

// A composite outcome is a success when the responder's own repair backs
// one cause, but a cause SDO verified as unattributed (someone else fixed it)
// or contradicted must not be surfaced to the next responder as a known root
// cause. Records without a verification keep every cause.
func TestRelevantOutcomeEvidenceSurfacesOnlyConfirmedRootCauses(t *testing.T) {
	repository := t.TempDir()
	outcome := `{"incident_id":"composite","source_commit":"abc","classification":"success",` +
		`"findings":[{"detector_id":"health","rule_id":"missing","fingerprint":"missing:cfg","primary_resource":{"kind":"ConfigMap","name":"cfg"}}],` +
		`"confirmed_root_causes":[{"summary":"NetworkPolicy deny-all isolates frontend","resources":[{"kind":"NetworkPolicy","name":"deny-all"}]},` +
		`{"summary":"ConfigMap cfg deleted","resources":[{"kind":"ConfigMap","name":"cfg"}]}],` +
		`"diagnosis_verification":[{"summary":"NetworkPolicy deny-all isolates frontend","verdict":"confirmed"},` +
		`{"summary":"ConfigMap cfg deleted","verdict":"unattributed"}]}` + "\n" +
		`{"incident_id":"legacy","source_commit":"abc","classification":"success",` +
		`"findings":[{"detector_id":"health","rule_id":"missing","fingerprint":"missing:cfg","primary_resource":{"kind":"ConfigMap","name":"cfg"}}],` +
		`"confirmed_root_causes":[{"summary":"legacy cause","resources":[{"kind":"ConfigMap","name":"cfg"}]}]}` + "\n"
	recordOutcomes(t, repository, outcome)
	findings := []sdk.Finding{{
		DetectorID: "health", RuleID: "missing", Fingerprint: "missing:cfg",
		PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Name: "cfg"},
	}}

	got := relevantOutcomeEvidence(repository, findings, "abc", nil)

	if len(got) != 2 {
		t.Fatalf("both successes are still relevant: %#v", got)
	}
	if len(got[0].RootCauseSummaries) != 1 || got[0].RootCauseSummaries[0] != "legacy cause" {
		t.Fatalf("a record without verification keeps its causes: %#v", got[0].RootCauseSummaries)
	}
	if len(got[1].RootCauseSummaries) != 1 || got[1].RootCauseSummaries[0] != "NetworkPolicy deny-all isolates frontend" {
		t.Fatalf("only the confirmed cause may be surfaced: %#v", got[1].RootCauseSummaries)
	}
}
