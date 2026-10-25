package runtime

import (
	"reflect"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestPlaybookCoverageIncludesEveryMatchAndUnexplainedFinding(t *testing.T) {
	refs := []sdk.ObjectRef{{Kind: "Service", Namespace: "app", Name: "one"}, {Kind: "Service", Namespace: "app", Name: "two"}, {Kind: "Service", Namespace: "app", Name: "three"}}
	findings := []sdk.Finding{}
	for _, ref := range refs {
		findings = append(findings, sdk.Finding{DetectorID: "health", RuleID: ref.Name, Status: sdk.FindingActive, PrimaryResource: ref})
	}
	yes := true
	runs := []ProgramRun{}
	for i, id := range []string{"repair-b", "repair-a"} {
		runs = append(runs, ProgramRun{ProgramID: id, ProgramRevision: "rev", CanMitigate: &yes, Bindings: program.Bindings{Objects: program.ObjectBindings{"service": refs[i]}}, Verdict: program.Verdict{Result: program.Match}})
	}
	// The same binding may be diagnosed through multiple findings; it appears once.
	runs = append(runs, runs[0])
	runs = append(runs, ProgramRun{ProgramID: "other", Bindings: runs[0].Bindings, Verdict: program.Verdict{Result: program.Match}})
	for _, result := range []program.Result{program.NoMatch, program.Unknown, program.NeedsJudgement} {
		runs = append(runs, ProgramRun{ProgramID: string(result), Bindings: program.Bindings{Objects: program.ObjectBindings{"service": refs[2]}}, Verdict: program.Verdict{Result: result}})
	}
	coverage := buildPlaybookCoverage(findings, runs, nil, map[string]ProgramTrust{"repair-a": {VerifiedSuccesses: 2, Failures: 3}})
	if len(coverage.GetMatches()) != 3 || !reflect.DeepEqual(coverage.GetUnexplainedFindingFingerprints(), []string{FindingFingerprint(findings[2])}) {
		t.Fatalf("coverage=%+v", coverage)
	}
	if coverage.Matches[0].PlaybookId != "other" || coverage.Matches[1].VerifiedSuccesses != 2 || coverage.Matches[1].VerifiedFailures != 3 || !coverage.Matches[1].CanMitigate {
		t.Fatalf("order/counters=%+v", coverage.Matches)
	}
	for _, match := range coverage.Matches {
		if len(match.ExplainedFindingFingerprints) != 1 {
			t.Fatalf("duplicate diagnosis or missing join: %+v", match)
		}
	}
	for i, j := 0, len(runs)-1; i < j; i, j = i+1, j-1 {
		runs[i], runs[j] = runs[j], runs[i]
	}
	if !reflect.DeepEqual(coverage, buildPlaybookCoverage(findings, runs, nil, map[string]ProgramTrust{"repair-a": {VerifiedSuccesses: 2, Failures: 3}})) {
		t.Fatal("coverage depends on input ordering")
	}
}
