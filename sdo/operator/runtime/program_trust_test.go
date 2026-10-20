package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestProgramTrustResetsOnFailureOrProgramRevisionChange(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{}
	for _, revision := range []string{"tree-a", "tree-a", "tree-a", "tree-b", "tree-b"} {
		outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: revision, ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: revision}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"targetport"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}}})
	}
	stats := ProgramTrustStats(outcomes)["targetport"]
	if stats.VerifiedSuccesses != 2 || stats.Trusted || stats.LastChange != "tree-b" {
		t.Fatalf("revision reset: %+v", stats)
	}
	outcomes = append(outcomes, &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: "tree-c"}}})
	stats = ProgramTrustStats(outcomes)["targetport"]
	if stats.VerifiedSuccesses != 0 || stats.Failures != 0 || stats.LastChange != "tree-c" {
		t.Fatalf("edit without application did not reset: %+v", stats)
	}
	outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: "tree-c", PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"targetport"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}})
	stats = ProgramTrustStats(outcomes)["targetport"]
	if stats.VerifiedSuccesses != 0 || stats.Failures != 1 || stats.Trusted {
		t.Fatalf("failure reset: %+v", stats)
	}
}
