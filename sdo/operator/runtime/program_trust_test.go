package runtime

import (
	"encoding/json"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestProgramTrustResetsOnFailureOrProgramRevisionChange(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{}
	for _, revision := range []string{"tree-a", "tree-a", "tree-a", "tree-b", "tree-b"} {
		outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", revision))
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
	failed := verifiedTrustOutcome(t, "targetport", "tree-c")
	failed.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_MATCH
	outcomes = append(outcomes, failed)
	stats = ProgramTrustStats(outcomes)["targetport"]
	if stats.VerifiedSuccesses != 0 || stats.Failures != 1 || stats.Trusted {
		t.Fatalf("failure reset: %+v", stats)
	}
}

func TestProgramTrustUsesConfiguredSuccessThreshold(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{verifiedTrustOutcome(t, "targetport", "tree-a"), verifiedTrustOutcome(t, "targetport", "tree-a")}
	if ProgramTrustStats(outcomes)["targetport"].Trusted || !ProgramTrustStats(outcomes, 2)["targetport"].Trusted {
		t.Fatal("configured trust threshold was ignored")
	}
}

func TestInterruptedFastPathFailureBreaksTrustWithoutApplication(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{}
	for index := 0; index < 3; index++ {
		outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", "tree-a"))
	}
	outcomes = append(outcomes, &contracts.OutcomeRecord{SourceCommit: "tree-a", ProgramFailureSignals: []string{"fast_path_failure:targetport"}})
	state := ProgramTrustStats(outcomes)["targetport"]
	if state.Trusted || state.Failures != 1 || state.VerifiedSuccesses != 0 {
		t.Fatalf("interrupted attempt did not break trust: %+v", state)
	}
}

func TestEquivalentResponderRepairEarnsTrustOnlyAfterIndependentSuccess(t *testing.T) {
	outcome := boundTrustOutcome(t, "equivalent-repair-1", "partial", program.NoMatch)
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; state.VerifiedSuccesses != 0 {
		t.Fatalf("unverified incident earned trust: %+v", state)
	}
	outcome.Classification = "success"
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; state.VerifiedSuccesses != 1 || !state.Trusted {
		t.Fatalf("verified equivalent repair earned no trust: %+v", state)
	}
}

func verifiedTrustOutcome(t *testing.T, id, revision string) *contracts.OutcomeRecord {
	t.Helper()
	outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
	outcome.SourceCommit = revision
	outcome.ProgramRuns[0].ProgramId, outcome.ProgramRuns[0].ProgramRevision = id, revision
	outcome.PlanApplications[0].ProgramIds = []string{id}
	for _, call := range outcome.ProgramToolCalls {
		call.RequestJson = strings.ReplaceAll(call.RequestJson, "targetport", id)
		call.ResponseJson = strings.ReplaceAll(strings.ReplaceAll(call.ResponseJson, "targetport", id), "revision-a", revision)
	}
	return outcome
}

// A controller-owned apply response ties the diagnosis to both the selected
// binding and the completed application. A health-only application verdict
// cannot supply that evidence.
func boundTrustOutcome(t *testing.T, kind, classification string, result program.Result) *contracts.OutcomeRecord {
	t.Helper()
	completed := time.Unix(100, 0).UTC()
	bindings := program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	return &contracts.OutcomeRecord{
		Classification: classification, SourceCommit: "tree-a",
		ProgramRuns:      []*contracts.ProgramRun{programRunContract(ProgramRun{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: program.Match}})},
		PlanApplications: []*contracts.PlanApplication{{ApplicationId: kind, Applied: true, ProgramIds: []string{"targetport"}, CompletedAt: timestamppb.New(completed), Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
		ProgramToolCalls: []*contracts.ProgramToolCall{
			{Command: "plan", At: timestamppb.New(completed.Add(-time.Second)), RequestJson: encode(ToolRequest{Command: "plan", ProgramID: "targetport", Bindings: bindings}), ResponseJson: encode(ToolResponse{Command: "plan", PlanID: "plan-1"})},
			{Command: "apply", At: timestamppb.New(completed.Add(time.Second)), RequestJson: encode(ToolRequest{Command: "apply", PlanIDs: []string{"plan-1"}}), ResponseJson: encode(ToolResponse{Command: "apply", ApplicationID: kind, Healthy: true, ProgramRuns: []ProgramRun{{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: result}}}})},
		},
	}
}

func TestProgramTrustRequiresIndependentSuccessForEveryApplicationKind(t *testing.T) {
	for _, kind := range []string{"application-1", "fast-path-1", "equivalent-repair-1", "learned-repair-targetport"} {
		for _, classification := range []string{"partial", "failed", "cancelled", ""} {
			t.Run(kind+"/"+classification, func(t *testing.T) {
				outcome := boundTrustOutcome(t, kind, classification, program.NoMatch)
				if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; got.VerifiedSuccesses != 0 || got.Trusted {
					t.Fatalf("locally green apply in unsuccessful outcome earned trust: %+v", got)
				}
			})
		}
	}
}

func TestProgramTrustRequiresBoundPostApplyNoMatch(t *testing.T) {
	for _, result := range []program.Result{program.Unknown, program.NeedsJudgement, program.Match} {
		t.Run(string(result), func(t *testing.T) {
			outcome := boundTrustOutcome(t, "application-1", "success", result)
			if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; got.VerifiedSuccesses != 0 || got.Trusted {
				t.Fatalf("inconclusive or failed post-apply diagnosis earned trust: %+v", got)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*contracts.OutcomeRecord)
	}{
		{"missing", func(o *contracts.OutcomeRecord) { o.ProgramToolCalls = nil }},
		{"before-apply", func(o *contracts.OutcomeRecord) { o.ProgramToolCalls[1].At = timestamppb.New(time.Unix(99, 0)) }},
		{"wrong-binding", func(o *contracts.OutcomeRecord) {
			o.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(o.ProgramToolCalls[1].ResponseJson, "api", "other")
		}},
		{"wrong-revision", func(o *contracts.OutcomeRecord) {
			o.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(o.ProgramToolCalls[1].ResponseJson, "revision-a", "revision-b")
		}},
		{"wrong-application", func(o *contracts.OutcomeRecord) {
			o.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(o.ProgramToolCalls[1].ResponseJson, "application-1", "application-2")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
			tc.mutate(outcome)
			if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; got.Trusted || got.VerifiedSuccesses != 0 {
				t.Fatalf("unbound evidence earned trust: %+v", got)
			}
		})
	}
	outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]; !got.Trusted || got.VerifiedSuccesses != 1 {
		t.Fatalf("independently successful bound repair earned no trust: %+v", got)
	}
}

func TestInconclusivePostApplyDiagnosisBreaksEarlierTrustStreak(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{verifiedTrustOutcome(t, "targetport", "revision-a")}
	outcomes = append(outcomes, boundTrustOutcome(t, "application-1", "success", program.Unknown))
	if got := ProgramTrustStats(outcomes, 1)["targetport"]; got.Trusted || got.VerifiedSuccesses != 0 {
		t.Fatalf("inconclusive apply preserved earlier trust: %+v", got)
	}
}

func TestProgramTrustCreditsAnOutcomeAtMostOnce(t *testing.T) {
	outcome := verifiedTrustOutcome(t, "targetport", "revision-a")
	outcome.PlanApplications = append(outcome.PlanApplications, outcome.PlanApplications[0])
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 2)["targetport"]; got.Trusted || got.VerifiedSuccesses != 1 {
		t.Fatalf("one outcome supplied multiple success credits: %+v", got)
	}
}
