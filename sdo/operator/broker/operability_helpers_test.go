package broker

import (
	"strings"
	"testing"

	"sdo.dev/operator/operability"
)

// deploymentReportFor is a passing stage-deployment report bound to evidence
// and to the commit its proposal was based on.
func deploymentReportFor(t *testing.T, evidence SourceEvidence, base string) operability.Report {
	t.Helper()
	subject := operability.Subject{
		AcceptedCommit: base, BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree,
		RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest, SettingsDigest: testSettingsDigest,
	}
	report, err := operability.New(operability.StageDeployment, subject).With(passingChecks("O1.deploy", "O2.stamps", "O6.budget")...)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func passingChecks(ids ...string) []operability.Check {
	checks := make([]operability.Check, 0, len(ids))
	for _, id := range ids {
		checks = append(checks, operability.Check{ID: id, Phase: operability.PhaseLive, Status: operability.StatusPass})
	}
	return checks
}

// A report made under one self-heal hold does not bind to a broker running under another: the digest of the
// detection section is part of the subject.
func TestReportsBindOnlyToTheDetectionSettingsTheyWereMadeUnder(t *testing.T) {
	evidence := SourceEvidence{BaseTree: "b", EffectiveTree: "e", RenderDigest: "r", EnvDigest: "v"}
	report := deploymentReportFor(t, evidence, "base")
	if _, err := deploymentReportDigest(report, evidence, testSettingsDigest); err != nil {
		t.Fatalf("same settings must bind: %v", err)
	}
	if _, err := deploymentReportDigest(report, evidence, "another-hold"); err == nil || !strings.Contains(err.Error(), "settings_digest") {
		t.Fatalf("a report made under another hold must not bind, got %v", err)
	}
}
