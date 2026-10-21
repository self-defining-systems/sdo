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

func TestProgramTrustUsesConfiguredSuccessThreshold(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{}
	for i := 0; i < 2; i++ {
		outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: "tree-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: "tree-a"}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"targetport"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}}})
	}
	if ProgramTrustStats(outcomes)["targetport"].Trusted || !ProgramTrustStats(outcomes, 2)["targetport"].Trusted {
		t.Fatal("configured trust threshold was ignored")
	}
}

func TestInterruptedFastPathFailureBreaksTrustWithoutApplication(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{}
	for index := 0; index < 3; index++ {
		outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: "tree-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: "tree-a"}}, PlanApplications: []*contracts.PlanApplication{{Applied: true, ProgramIds: []string{"targetport"}, Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}}})
	}
	outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: "tree-a", ProgramFailureSignals: []string{"fast_path_failure:targetport"}})
	state := ProgramTrustStats(outcomes)["targetport"]
	if state.Trusted || state.Failures != 1 || state.VerifiedSuccesses != 0 {
		t.Fatalf("interrupted attempt did not break trust: %+v", state)
	}
}
