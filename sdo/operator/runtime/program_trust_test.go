package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestEquivalentResponderRepairEarnsTrustOnlyAfterIndependentSuccess(t *testing.T) {
	outcome := boundTrustOutcome(t, "equivalent-repair-1", "failed", program.NoMatch)
	if state := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; state.VerifiedSuccesses != 0 {
		t.Fatalf("unverified incident earned trust: %+v", state)
	}
	outcome.Classification = "success"
	if state := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; state.VerifiedSuccesses != 1 || (state.VerifiedSuccesses == 0 || state.Failures != 0) {
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
	settleTestOutcome(outcome)
	return outcome
}

// A controller-owned apply response ties the diagnosis to both the selected
// binding and the completed application. A health-only application verdict
// cannot supply that evidence.
func boundTrustOutcome(t *testing.T, kind, classification string, result program.Result) *contracts.OutcomeRecord {
	t.Helper()
	completed := time.Unix(100, 0).UTC()
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	return &contracts.OutcomeRecord{
		Classification: classification, SourceCommit: "tree-a",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{objectRefContract(bindings.Objects["service"])}}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{objectRefContract(bindings.Objects["service"])}}},
		ProgramRuns:         []*contracts.ProgramRun{programRunContract(ProgramRun{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: program.Match}})},
		PlanApplications:    []*contracts.PlanApplication{{ApplicationId: kind, Applied: true, VerificationCompleted: true, VerificationPassed: true, FinalEquivalenceVerified: proto.Bool(true), ProgramIds: []string{"targetport"}, CompletedAt: timestamppb.New(completed), Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}}},
		ProgramToolCalls: []*contracts.ProgramToolCall{
			{Command: "plan", At: timestamppb.New(completed.Add(-time.Second)), RequestJson: encode(ToolRequest{Command: "plan", ProgramID: "targetport", Bindings: bindings}), ResponseJson: encode(ToolResponse{Command: "plan", PlanID: "plan-1"})},
			{Command: "apply", At: timestamppb.New(completed.Add(time.Second)), RequestJson: encode(ToolRequest{Command: "apply", PlanIDs: []string{"plan-1"}}), ResponseJson: encode(ToolResponse{Command: "apply", ApplicationID: kind, Healthy: true, ProgramRuns: []ProgramRun{{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: result}}}})},
		},
	}
}

func TestProgramTrustRequiresIndependentSuccessForEveryApplicationKind(t *testing.T) {
	for _, kind := range []string{"application-1", "mitigation-1", "equivalent-repair-1", "learned-repair-targetport"} {
		for _, classification := range []string{"failed", "cleared_without_action", ""} {
			t.Run(kind+"/"+classification, func(t *testing.T) {
				outcome := boundTrustOutcome(t, kind, classification, program.NoMatch)
				if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; got.VerifiedSuccesses != 0 || (got.VerifiedSuccesses != 0 && got.Failures == 0) {
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
			if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; got.VerifiedSuccesses != 0 || (got.VerifiedSuccesses != 0 && got.Failures == 0) {
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
			if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.VerifiedSuccesses != 0 {
				t.Fatalf("unbound evidence earned trust: %+v", got)
			}
		})
	}
	outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses == 0 || got.Failures != 0) || got.VerifiedSuccesses != 1 {
		t.Fatalf("independently successful bound repair earned no trust: %+v", got)
	}
}

func TestInconclusivePostApplyDiagnosisCannotExtendEarlierTrustStreak(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{verifiedTrustOutcome(t, "targetport", "revision-a")}
	outcomes = append(outcomes, boundTrustOutcome(t, "application-1", "success", program.Unknown))
	if got := settledTrustStats(outcomes)["targetport"]; got.VerifiedSuccesses != 1 {
		t.Fatalf("inconclusive apply changed prior credit: %+v", got)
	}
}

func TestProgramTrustCreditsAnOutcomeAtMostOnce(t *testing.T) {
	outcome := verifiedTrustOutcome(t, "targetport", "revision-a")
	outcome.PlanApplications = append(outcome.PlanApplications, outcome.PlanApplications[0])
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; got.VerifiedSuccesses != 1 {
		t.Fatalf("one outcome supplied multiple success credits: %+v", got)
	}
}

func TestFailedProgramRevisionStaysSuspiciousAfterReversion(t *testing.T) {
	outcomes := []*contracts.OutcomeRecord{verifiedTrustOutcome(t, "targetport", "revision-a")}
	outcomes = append(outcomes, &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: "revision-a"}}, ProgramFailureSignals: []string{"application_verification_failure:targetport"}})
	outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", "revision-b"))
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", "revision-a"))
	}
	if got := settledTrustStats(outcomes)["targetport"]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.Failures == 0 {
		t.Fatalf("reverting to failed revision restored eligibility: %+v", got)
	}
}

func TestProgramTrustRejectsDiagnosisFromDifferentAPIGroup(t *testing.T) {
	outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
	outcome.ProgramToolCalls[1].ResponseJson = strings.ReplaceAll(outcome.ProgramToolCalls[1].ResponseJson, `"api_version":"v1"`, `"api_version":"other.example/v1"`)
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.VerifiedSuccesses != 0 {
		t.Fatalf("different API group earned binding trust: %+v", got)
	}
}

func TestProgramTrustConsumesRealResponderApplyEvidence(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	run := loop.ProgramRuns()[0]
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: run.ProgramID, Bindings: run.Bindings})
	if planned.Error != "" || planned.PlanID == "" {
		t.Fatalf("plan failed: %+v", planned)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || !applied.Healthy {
		t.Fatalf("apply failed: %+v", applied)
	}
	outcome := &contracts.OutcomeRecord{SourceCommit: "tree-a", Classification: "success"}
	for _, run := range loop.ProgramRuns() {
		run.ProgramRevision = "tree-a"
		outcome.ProgramRuns = append(outcome.ProgramRuns, programRunContract(run))
	}
	for _, entry := range loop.PlanApplications() {
		application, err := entry.Result.Proto()
		if err != nil {
			t.Fatal(err)
		}
		application.FinalEquivalenceVerified = proto.Bool(true)
		application.ApplicationId, application.ProgramIds, application.CompletedAt = entry.ID, entry.ProgramIDs, timestamppb.New(entry.CompletedAt)
		outcome.PlanApplications = append(outcome.PlanApplications, application)
	}
	for _, call := range loop.ToolCalls() {
		outcome.ProgramToolCalls = append(outcome.ProgramToolCalls, toolCallContract(call))
	}
	outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{objectRefContract(run.Bindings.Objects["service"])}}}
	outcome.RepairActions = []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{objectRefContract(run.Bindings.Objects["service"])}}}
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})[run.ProgramID]; (got.VerifiedSuccesses == 0 || got.Failures != 0) || got.VerifiedSuccesses != 1 {
		t.Fatalf("real successful apply earned no trust: %+v", got)
	}
	outcome.Classification = "failed"
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})[run.ProgramID]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.VerifiedSuccesses != 0 {
		t.Fatalf("real locally green apply in controller-failed outcome earned trust: %+v", got)
	}
}

// Plan and apply responses carry patches, findings and per-run evidence, so
// real ones exceed the recorded-response cap. Trust must survive that.
func oversizedTrustOutcome(t *testing.T, runs int) *contracts.OutcomeRecord {
	t.Helper()
	outcome := boundTrustOutcome(t, "application-1", "success", program.NoMatch)
	bulk := strings.Repeat("x", 600)
	var plan ToolResponse
	var apply ToolResponse
	if err := json.Unmarshal([]byte(outcome.ProgramToolCalls[0].ResponseJson), &plan); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(outcome.ProgramToolCalls[1].ResponseJson), &apply); err != nil {
		t.Fatal(err)
	}
	plan.Summary = bulk
	for index := 0; index < 5; index++ {
		plan.Patches = append(plan.Patches, PlanPatch{Patch: json.RawMessage(`"` + bulk + `"`)})
	}
	apply.Summary = bulk
	bindings := apply.ProgramRuns[0].Bindings
	apply.ProgramRuns = nil
	for index := 0; index < runs; index++ {
		other := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: fmt.Sprintf("other-%d", index)}}}
		apply.ProgramRuns = append(apply.ProgramRuns, ProgramRun{ProgramID: fmt.Sprintf("other-program-%d", index), ProgramRevision: "revision-a", Bindings: other, Verdict: program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Resource: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fact: bulk, Observed: bulk}}}})
	}
	apply.ProgramRuns = append(apply.ProgramRuns, ProgramRun{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Resource: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fact: bulk, Observed: bulk}}}})
	applied := []selectedPlan{{programID: "targetport", bindings: bindings}}
	for index, response := range []ToolResponse{plan, apply} {
		full, _ := json.Marshal(response)
		if len(full) <= maxRecordedToolResponse {
			t.Fatalf("fixture response %d is %d bytes, not over the cap", index, len(full))
		}
		var recorded []byte
		if index == 1 {
			recorded = recordedToolResponseJSON(response, applied)
			t.Logf("apply response with %d programs: %d bytes full, %d bytes recorded", runs+1, len(full), len(recorded))
		} else {
			recorded = recordedToolResponseJSON(response, nil)
		}
		if len(recorded) > maxRecordedApplyResponse {
			t.Fatalf("recorded response %d is %d bytes, over the cap", index, len(recorded))
		}
		outcome.ProgramToolCalls[index].ResponseJson = string(recorded)
	}
	return outcome
}

func TestProgramTrustSurvivesOversizedPlanAndApplyRecords(t *testing.T) {
	outcome := oversizedTrustOutcome(t, 0)
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses == 0 || got.Failures != 0) || got.VerifiedSuccesses != 1 {
		t.Fatalf("truncated plan/apply records earned no trust: %+v", got)
	}
}

func TestProgramTrustCountsRepairWhenApplyResponseListsManyPrograms(t *testing.T) {
	outcome := oversizedTrustOutcome(t, 40)
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses == 0 || got.Failures != 0) || got.VerifiedSuccesses != 1 {
		t.Fatalf("unrelated programs' runs cost the applied program its trust: %+v", got)
	}
	var recorded struct {
		Omitted          int `json:"program_runs_omitted"`
		UnrelatedOmitted int `json:"program_runs_unrelated_omitted"`
	}
	if err := json.Unmarshal([]byte(outcome.ProgramToolCalls[1].ResponseJson), &recorded); err != nil || recorded.Omitted != 0 || recorded.UnrelatedOmitted != 40 {
		t.Fatalf("unrelated drops are not counted separately: %v %+v", err, recorded)
	}
}

func TestProgramTrustFailsClosedWhenRelevantRunsOverflow(t *testing.T) {
	outcome := oversizedTrustOutcome(t, 0)
	var apply ToolResponse
	if err := json.Unmarshal([]byte(outcome.ProgramToolCalls[1].ResponseJson), &apply); err != nil {
		t.Fatal(err)
	}
	var applied []selectedPlan
	apply.ProgramRuns = nil
	for index := 0; index < 200; index++ {
		bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: fmt.Sprintf("api-%d", index)}}}
		applied = append(applied, selectedPlan{programID: "targetport", bindings: bindings})
		apply.ProgramRuns = append(apply.ProgramRuns, ProgramRun{ProgramID: "targetport", ProgramRevision: "revision-a", Bindings: bindings, Verdict: program.Verdict{Result: program.NoMatch}})
	}
	apply.ProgramRuns = append(apply.ProgramRuns, ProgramRun{ProgramID: "unrelated", Verdict: program.Verdict{Result: program.NoMatch}})
	recorded := recordedToolResponseJSON(apply, applied)
	if len(recorded) > maxRecordedApplyResponse {
		t.Fatalf("recorded apply is %d bytes, over the hard cap", len(recorded))
	}
	var parsed ToolResponse
	if err := json.Unmarshal(recorded, &parsed); err != nil || parsed.ProgramRunsOmitted == 0 || parsed.ProgramRunsUnrelatedOmitted != 1 {
		t.Fatalf("dropped relevant runs are not marked: %v %+v", err, parsed)
	}
	outcome.ProgramToolCalls[1].ResponseJson = string(recorded)
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.VerifiedSuccesses != 0 {
		t.Fatalf("a dropped relevant run earned trust: %+v", got)
	}
}

func TestTruncatedRecordKeepsDiagnosisError(t *testing.T) {
	outcome := oversizedTrustOutcome(t, 0)
	var apply ToolResponse
	if err := json.Unmarshal([]byte(outcome.ProgramToolCalls[1].ResponseJson), &apply); err != nil {
		t.Fatal(err)
	}
	apply.ProgramRuns[0].Verdict.Evidence = append(apply.ProgramRuns[0].Verdict.Evidence, program.Evidence{Observed: "diagnosis crashed"})
	apply.Summary = strings.Repeat("x", 3000)
	outcome.ProgramToolCalls[1].ResponseJson = string(recordedToolResponseJSON(apply, []selectedPlan{{programID: "targetport", bindings: apply.ProgramRuns[0].Bindings}}))
	if got := settledTrustStats([]*contracts.OutcomeRecord{outcome})["targetport"]; (got.VerifiedSuccesses != 0 && got.Failures == 0) || got.VerifiedSuccesses != 0 {
		t.Fatalf("diagnosis error was dropped by truncation: %+v", got)
	}
}

// Fixtures model the controller settlement before the durable trust projection.
func settleTestOutcome(outcome *contracts.OutcomeRecord) {
	fixed := map[string]program.Result{}
	for _, run := range outcome.GetProgramRuns() {
		bindings := program.Bindings{Objects: program.ObjectBindings{}}
		for _, binding := range run.GetBindings() {
			bindings.Objects[binding.GetName()] = objectRefState(binding.GetObject())
		}
		fixed[matchedViewKey(run.GetProgramId(), bindings)] = program.NoMatch
	}
	outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: classifyCauseWork(outcome, fixed), Unassigned: unassignedReflectionQuestions(outcome, fixed)}
}
func settledTrustStats(outcomes []*contracts.OutcomeRecord) map[string]ProgramTrust {
	for _, outcome := range outcomes {
		settleTestOutcome(outcome)
	}
	return ProgramTrustStats(outcomes)
}
