package runtime

import (
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func coveredRepeatOutcome(t *testing.T) *contracts.OutcomeRecord {
	o := t1Outcome(t, "synthetic-revision")
	o.Findings = []*contracts.Finding{{Fingerprint: "routing", DetectorId: "routing", RuleId: "target-port", Status: "resolved", Severity: "warn", Summary: "synthetic routing mismatch", Evidence: "synthetic receipt", PrimaryResource: t1Ref("api")}}
	o.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{{PlaybookId: t1Program, ProgramRevision: "synthetic-revision", Bindings: o.ProgramRuns[0].Bindings, ExplainedFindingFingerprints: []string{"routing"}}}}
	return o
}

func TestReflectionNoveltyRequiresCompleteRecordedCoverage(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*contracts.OutcomeRecord)
	}{
		{"covered", func(*contracts.OutcomeRecord) {}},
		{"uncovered", func(o *contracts.OutcomeRecord) {
			o.Findings = append(o.Findings, &contracts.Finding{Fingerprint: "new"})
		}},
		{"failed", func(o *contracts.OutcomeRecord) { o.Classification = "failed" }},
		{"partial", func(o *contracts.OutcomeRecord) { o.PlanApplications[0].VerificationPassed = false }},
		{"unknown", func(o *contracts.OutcomeRecord) {
			o.PlanApplications[0].Verification.Result = contracts.Verdict_RESULT_UNKNOWN
		}},
		{"new transition", func(o *contracts.OutcomeRecord) { o.UndetectedCauses = []*contracts.UndetectedCause{{}} }},
		{"new detector evidence", func(o *contracts.OutcomeRecord) { o.DisabledSetupDetectors = []*contracts.DisabledSetupDetector{{}} }},
		{"new source", func(o *contracts.OutcomeRecord) { o.RepairCommit = proto.String("source-repair") }},
		{"program failure", func(o *contracts.OutcomeRecord) { o.ProgramFailureSignals = []string{"failed"} }},
		{"no findings", func(o *contracts.OutcomeRecord) { o.Findings = nil }},
		{"no application", func(o *contracts.OutcomeRecord) { o.PlanApplications = nil }},
		{"empty fingerprint", func(o *contracts.OutcomeRecord) { o.Findings[0].Fingerprint = "" }},
		{"wrong binding", func(o *contracts.OutcomeRecord) {
			o.PlaybookCoverage.Matches[0].Bindings = []*contracts.Binding{{Name: "service", Object: t1Ref("peer")}}
		}},
		{"wrong revision", func(o *contracts.OutcomeRecord) { o.PlaybookCoverage.Matches[0].ProgramRevision = "peer-revision" }},
		{"missing post-apply proof", func(o *contracts.OutcomeRecord) { o.ProgramToolCalls = nil }},
		{"unapplied coverage", func(o *contracts.OutcomeRecord) { o.PlaybookCoverage.Matches[0].PlaybookId = "peer" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := coveredRepeatOutcome(t)
			test.change(o)
			if got := coveredRepeatCandidate(o); got != (test.name == "covered") {
				t.Fatalf("covered repeat = %v", got)
			}
		})
	}
}

