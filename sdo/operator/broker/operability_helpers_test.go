package broker

import (
	"testing"

	"sdo.dev/operator/operability"
)

// deploymentReportFor is a passing stage-deployment report bound to evidence
// and to the commit its proposal was based on.
func deploymentReportFor(t *testing.T, evidence SourceEvidence, base string) operability.Report {
	t.Helper()
	subject := operability.Subject{
		AcceptedCommit: base, BaseTree: evidence.BaseTree, EffectiveTree: evidence.EffectiveTree,
		PatchesDigest: evidence.PatchesDigest, RenderDigest: evidence.RenderDigest, EnvDigest: evidence.EnvDigest,
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
