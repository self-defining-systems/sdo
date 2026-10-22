package runtime

import (
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestCauseWorkKeepsConfirmedGroupsAndNeverCreditsUncertainRepairs(t *testing.T) {
	original := verifiedRepeat(t)
	original.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
	for _, test := range []struct {
		name                string
		change              func(*contracts.OutcomeRecord)
		fixed               program.Result
		disposition, reason string
	}{
		{"covered", func(*contracts.OutcomeRecord) {}, program.NoMatch, "covered", "verified_equivalent_repair"},
		{"false match", func(*contracts.OutcomeRecord) {}, program.Match, "false_match", "fixed_capture_match"},
		{"unwritten", func(o *contracts.OutcomeRecord) {
			o.RepairActions = nil
			o.PlanApplications = nil
			o.ProgramToolCalls = nil
		}, program.NoMatch, "question", "binding_not_written"},
		{"different repair", func(o *contracts.OutcomeRecord) {
			o.PlanApplications = nil
			o.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
			o.RepairActions[0].Kind = "manual"
		}, program.NoMatch, "question", "repair_equivalence_question"},
		{"unknown", func(*contracts.OutcomeRecord) {}, program.Unknown, "question", "uncertain"},
		{"unverified fixed match", func(o *contracts.OutcomeRecord) { o.Classification = "failure" }, program.Match, "question", "uncertain"},
		{"unrecorded unwritten binding", func(o *contracts.OutcomeRecord) { o.RepairActions = nil; o.PlanApplications = nil }, "", "question", "uncertain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := proto.Clone(original).(*contracts.OutcomeRecord)
			test.change(outcome)
			work := classifyCauseWork(outcome, map[string]program.Result{matchedViewKey(repeatProgram, program.Bindings{"service": objectRefState(repeatService)}): test.fixed})
			if len(work) != 1 || work[0].GetCauseIndex() != 0 || !proto.Equal(work[0].Resources[0], repeatService) {
				t.Fatalf("cause grouping changed: %v", work)
			}
			item := work[0].Items[0]
			if item.Disposition != test.disposition || item.Reason != test.reason {
				t.Fatalf("work=%v", item)
			}
		})
	}
}

func TestUnassignedMatchesStayQuestionsWithoutNewCauseOrTrustCredit(t *testing.T) {
	outcome := verifiedRepeat(t)
	outcome.ConfirmedRootCauses = nil
	questions := unassignedReflectionQuestions(outcome, nil)
	if len(questions) != 1 || questions[0].Disposition != "question" || len(classifyCauseWork(outcome, nil)) != 0 {
		t.Fatalf("inferred a cause: %v", questions)
	}

}

func TestUnprovenFinalEquivalenceEarnsNeitherSuccessNorFailure(t *testing.T) {
	outcome := verifiedTrustOutcome(t, "targetport", "revision")
	notExact := false
	outcome.PlanApplications[0].FinalEquivalenceVerified = &notExact
	settleTestOutcome(outcome)
	state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["targetport"]
	if state.Trusted || state.VerifiedSuccesses != 0 || state.Failures != 0 {
		t.Fatalf("question became a trust judgment: %+v", state)
	}
}

func TestUnboundWritesKeepTheExactConfirmedResource(t *testing.T) {
	outcome := verifiedRepeat(t)
	outcome.ProgramRuns = nil
	outcome.PlanApplications = nil
	outcome.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
	work := classifyCauseWork(outcome, nil)
	if len(work) != 1 || len(work[0].Items) != 1 || work[0].Items[0].Reason != "unbound_write" ||
		!proto.Equal(work[0].Items[0].Resources[0], repeatService) {
		t.Fatalf("unbound write lost its resource: %v", work)
	}
}

func TestMissingProofAndFixedVerdictRemainQuestions(t *testing.T) {
	for _, test := range []struct {
		name  string
		proof bool
		fixed program.Result
	}{
		{"missing final proof", false, program.NoMatch}, {"missing fixed verdict", true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := verifiedRepeat(t)
			o.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
			if test.proof {
				o.PlanApplications[0].FinalEquivalenceVerified = proto.Bool(true)
			} else {
				o.PlanApplications[0].FinalEquivalenceVerified = nil
			}
			key := matchedViewKey(repeatProgram, program.Bindings{"service": objectRefState(repeatService)})
			work := classifyCauseWork(o, map[string]program.Result{key: test.fixed})
			if work[0].Items[0].Disposition != "question" {
				t.Fatalf("unproven case settled: %v", work)
			}
		})
	}
}
