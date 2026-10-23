package runtime

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk/program"
)

func TestRefinementQuestionNamesAbstainingEvaluation(t *testing.T) {
	for _, verdict := range []contracts.Verdict_Result{contracts.Verdict_RESULT_NO_MATCH, contracts.Verdict_RESULT_UNKNOWN} {
		t.Run(verdict.String(), func(t *testing.T) {
			outcome := t1Outcome(t, "accepted-revision")
			outcome.PlanApplications, outcome.ProgramToolCalls = nil, nil
			run := outcome.ProgramRuns[0]
			run.Verdict = &contracts.Verdict{Result: verdict, Evidence: []*contracts.Evidence{{Resource: t1Ref("api"), Fact: "observation unavailable"}}}
			run.Error = "read unavailable"
			outcome.ProgramRuns = append(outcome.ProgramRuns, proto.Clone(run).(*contracts.ProgramRun))
			before := proto.Clone(outcome)
			work := classifyCauseWork(outcome, nil)
			if len(work) != 1 || len(work[0].Items) != 1 {
				t.Fatalf("expected one deduplicated candidate, got %v", work)
			}
			item := work[0].Items[0]
			if item.Disposition != "question" || item.Reason != "program_refinement" || item.ProgramId != t1Program || item.ProgramRevision != "accepted-revision" {
				t.Fatalf("missing typed refinement candidate: %v", item)
			}
			if !strings.Contains(item.Question, t1Program) || !strings.Contains(item.Question, verdict.String()) || !strings.Contains(item.Question, "positive case") {
				t.Errorf("question must name evaluation and ask about incident positive case: %s", item.Question)
			}
			if !proto.Equal(item.Evaluation, run) || !proto.Equal(item.Bindings[0], run.Bindings[0]) || !proto.Equal(item.Resources[0], t1Ref("api")) || item.EvidencePhase != "open" {
				t.Fatalf("refinement must retain evaluation, bindings, repaired object and positive capture phase: %v", item)
			}
			if err := wire.Validate(item); err != nil {
				t.Fatalf("refinement question violates the worker contract: %v", err)
			}
			item.Evaluation.Error = "changed copy"
			item.Bindings[0].Object.Name = "changed copy"
			if !proto.Equal(before, outcome) {
				t.Fatal("classification mutated the authoritative outcome")
			}
			positive, negative := settledProgramCases(&contracts.ReflectionEligibility{Version: 1, Causes: work})
			if len(positive) != 0 || len(negative) != 0 || !causeWorkNeedsModel(work) {
				t.Fatalf("a refinement question cannot settle corpus cases or grant credit: %v / %v", positive, negative)
			}
		})
	}
}

func TestRefinementQuestionKeepsDistinctRevisionsAndVerdicts(t *testing.T) {
	outcome := t1Outcome(t, "revision-a")
	outcome.ProgramRuns[0].Verdict.Result = contracts.Verdict_RESULT_UNKNOWN
	other := proto.Clone(outcome.ProgramRuns[0]).(*contracts.ProgramRun)
	other.ProgramRevision = "revision-b"
	noMatch := proto.Clone(other).(*contracts.ProgramRun)
	noMatch.Verdict.Result = contracts.Verdict_RESULT_NO_MATCH
	outcome.ProgramRuns = append(outcome.ProgramRuns, other, noMatch)
	work := classifyCauseWork(outcome, nil)
	if len(work[0].Items) != 3 {
		t.Fatalf("distinct revisions and verdicts must remain visible: %v", work)
	}
}

func TestRefinementQuestionRequiresSameRepairedCauseObject(t *testing.T) {
	for _, change := range []string{"unwritten", "failed_write", "other_object", "other_namespace", "other_kind", "outside_cause", "missing_verdict", "needs_judgement"} {
		t.Run(change, func(t *testing.T) {
			outcome := t1Outcome(t, "revision")
			outcome.ProgramRuns[0].Verdict.Result = contracts.Verdict_RESULT_UNKNOWN
			switch change {
			case "unwritten":
				outcome.RepairActions = nil
			case "failed_write":
				outcome.RepairActions[0].Success = false
			case "other_object":
				outcome.ProgramRuns[0].Bindings[0].Object.Name = "other"
			case "other_namespace":
				outcome.ProgramRuns[0].Bindings[0].Object.Namespace = "other"
			case "other_kind":
				outcome.ProgramRuns[0].Bindings[0].Object.Kind = "Deployment"
			case "outside_cause":
				outcome.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{t1Ref("other")}
			case "missing_verdict":
				outcome.ProgramRuns[0].Verdict = nil
			case "needs_judgement":
				outcome.ProgramRuns[0].Verdict.Result = contracts.Verdict_RESULT_NEEDS_JUDGEMENT
			}
			for _, cause := range classifyCauseWork(outcome, nil) {
				for _, item := range cause.Items {
					if item.Reason == "program_refinement" {
						t.Fatalf("unrelated abstention became a refinement candidate: %v", item)
					}
				}
			}
		})
	}
}

func TestRefinementQuestionDoesNotTreatPostRepairNoMatchAsMissedCoverage(t *testing.T) {
	for _, revision := range []string{"revision", ""} {
		t.Run("fixed_revision="+revision, func(t *testing.T) {
			outcome := t1Outcome(t, "revision")
			fixed := proto.Clone(outcome.ProgramRuns[0]).(*contracts.ProgramRun)
			fixed.ProgramRevision = revision
			fixed.Verdict.Result = contracts.Verdict_RESULT_NO_MATCH
			outcome.ProgramRuns = append(outcome.ProgramRuns, fixed)
			key := matchedViewKey(t1Program, program.Bindings{Objects: program.ObjectBindings{"service": objectRefState(t1Ref("api"))}})
			work := classifyCauseWork(outcome, map[string]program.Result{key: program.NoMatch})
			if len(work[0].Items) != 1 || work[0].Items[0].Reason == "program_refinement" {
				t.Fatalf("post-repair abstention must retain existing matched work only: %v", work)
			}
			if revision != "" && work[0].Items[0].Disposition != "covered" {
				t.Fatalf("verified existing coverage must remain settled: %v", work)
			}
		})
	}
}
