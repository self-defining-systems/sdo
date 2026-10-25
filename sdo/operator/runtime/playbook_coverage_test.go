package runtime

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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

func TestPlaybookCoverageJoinsRelatedAndParameterResources(t *testing.T) {
	bound := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	unrelated := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "other"}
	findings := []sdk.Finding{
		{Fingerprint: "primary", Status: sdk.FindingActive, PrimaryResource: bound},
		{Fingerprint: "related", Status: sdk.FindingActive, PrimaryResource: unrelated, RelatedResources: []sdk.ObjectRef{bound}},
		{Fingerprint: "parameter", Status: sdk.FindingActive, PrimaryResource: unrelated, ParameterBindings: map[string]sdk.ObjectRef{"workload": bound}},
		{Fingerprint: "foreign", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "elsewhere", Name: "api"}},
	}
	runs := []ProgramRun{{ProgramID: "repair", Bindings: program.Bindings{Objects: program.ObjectBindings{"workload": bound}}, Verdict: program.Verdict{Result: program.Match}}}
	coverage := buildPlaybookCoverage(findings, runs, nil, nil)
	if !reflect.DeepEqual(coverage.Matches[0].ExplainedFindingFingerprints, []string{"parameter", "primary", "related"}) || !reflect.DeepEqual(coverage.UnexplainedFindingFingerprints, []string{"foreign"}) {
		t.Fatalf("resource joins: %v", coverage)
	}
}

func TestPlaybookCoverageJoinsWorkloadServiceRelation(t *testing.T) {
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}}}}}, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}}}}}
	findings := []sdk.Finding{{Fingerprint: "service-symptom", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
	runs := []ProgramRun{{ProgramID: "repair", Bindings: program.Bindings{Objects: program.ObjectBindings{"workload": {Kind: "Deployment", Namespace: "app", Name: "api"}}}, Verdict: program.Verdict{Result: program.Match}}}
	coverage := buildPlaybookCoverage(findings, runs, snapshot, nil)
	if len(coverage.UnexplainedFindingFingerprints) != 0 || !reflect.DeepEqual(coverage.Matches[0].ExplainedFindingFingerprints, []string{"service-symptom"}) {
		t.Fatalf("relation join: %v", coverage)
	}
}

func TestPlaybookCoverageSweepBindingIsOfferedByMenu(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.Findings = nil
	canMitigate := true
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": menuService, "deployment": menuDeployment}}
	coverage := buildPlaybookCoverage(nil, []ProgramRun{{ProgramID: "routing-then-restart", CanMitigate: &canMitigate, Bindings: bindings, Verdict: program.Verdict{Result: program.Match}}}, nil, nil)
	loop.Request.PlaybookCoverage = coverage
	menu := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	for _, option := range menu.Menu.Options {
		if option.OptionId == coverage.Matches[0].MitigationSelection {
			return
		}
	}
	t.Fatalf("coverage selection absent from menu: %v", menu.Menu)
}

// C4 filters retired programs at the loader. Coverage must not resurrect an
// unloaded playbook from a stale finding hint or surfaced README reference.
func TestPlaybookCoverageDoesNotOfferUnloadedPlaybooks(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	loop.Request.Findings[0].Playbooks = []string{"retired-playbook"}
	runs := loop.InitialDiagnose(context.Background())
	coverage := buildPlaybookCoverage(loop.Request.Findings, runs, loop.lastDiagnosisView, nil)
	if len(coverage.Matches) != 1 || coverage.Matches[0].PlaybookId != "service-targetport-mismatch" {
		t.Fatalf("coverage offered an unloaded playbook: %v", coverage)
	}
}
