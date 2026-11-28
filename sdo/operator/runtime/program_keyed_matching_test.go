package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func keyedDeployment(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name}
}

// keyedFinding is a user-test failure whose resources name every suspect, with
// no playbook list: the shape a symptom-keyed playbook must still reach.
func keyedFinding(suspects ...string) sdk.Finding {
	finding := sdk.Finding{DetectorID: "traffic", RuleID: "scenario-slo.browse", Status: sdk.FindingActive, Fingerprint: "browse",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "gateway"}}
	for _, name := range suspects {
		finding.RelatedResources = append(finding.RelatedResources, keyedDeployment(name))
	}
	return finding
}

func keyedRegistry(t *testing.T, matching func(program.Bindings) program.Result, ids ...string) program.ProgramRegistry {
	t.Helper()
	var diagnosers []program.Diagnoser
	for _, id := range ids {
		diagnosers = append(diagnosers, candidateProgram{spec: program.ProgramSpec{ID: id, Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}}},
			run: func(_ context.Context, b program.Bindings) program.Verdict {
				return program.Verdict{Result: matching(b)}
			}})
	}
	registry, err := program.NewRegistry(diagnosers, nil)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func keyedView(names ...string) program.ReadView {
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	for _, name := range names {
		snapshot.DeploymentList = append(snapshot.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}})
	}
	return &sdktest.ReplayReadView{Snapshot: snapshot}
}

func matchAll(program.Bindings) program.Result { return program.Match }

func boundNames(runs []ProgramRun, id string) []string {
	var names []string
	for _, run := range runs {
		if run.ProgramID == id {
			names = append(names, run.Bindings.Objects[ComponentRole].Name)
		}
	}
	return names
}

func credits(id, symptom string, components ...string) ProgramCredits {
	result := ProgramCredits{}
	for _, name := range components {
		result[id] = append(result[id], CreditKey{Symptom: symptom, Component: keyedDeployment(name)})
	}
	return result
}

// A finding naming two same-Kind suspects binds no single-role playbook by
// itself; the credited pair picks its component, and the other suspect, with no
// credited pair of its own, is never bound.
func TestCreditedPairBindsOnlyTheCreditedSuspect(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix")
	findings := []sdk.Finding{keyedFinding("front", "api")}
	keyed := KeyedMatching{Credits: credits("fix", "k1", "front"), OpeningKeys: []string{"k1"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("front", "api"), findings, time.Second, keyed)
	if got := boundNames(runs, "fix"); !slices.Equal(got, []string{"front"}) {
		t.Fatalf("bound %v, want only the credited front", got)
	}
	if runs[0].BindingSource != "credited" || runs[0].FindingFingerprint != "browse" {
		t.Fatalf("run = %+v, want source credited and the finding that named the suspect", runs[0])
	}
	// Without the credit, today's binding cannot choose between two Deployments.
	if runs := SelectProgramCandidates(context.Background(), registry, keyedView("front", "api"), findings, time.Second); len(runs) != 0 {
		t.Fatalf("no credit: bound %+v, want none (two same-Kind resources)", runs)
	}
}

func TestCreditedPairNeedsItsSymptomOpen(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix")
	keyed := KeyedMatching{Credits: credits("fix", "k1", "front"), OpeningKeys: []string{"another-alert"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("front", "api"), []sdk.Finding{keyedFinding("front", "api")}, time.Second, keyed)
	if len(runs) != 0 {
		t.Fatalf("a pair credited for another symptom bound %+v", runs)
	}
}

// Credits rank candidate bindings and never filter them: a playbook credited
// on one component still binds a sibling suspect through the finding, so the
// stage applies it when its Diagnose matches there and every parameter binds.
func TestStageBindsACreditedPlaybookToANewSuspect(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix")
	findings := []sdk.Finding{keyedFinding("store-b")}
	keyed := KeyedMatching{Credits: credits("fix", "k1", "store-a"), OpeningKeys: []string{"k1"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("store-b"), findings, time.Second, keyed)
	if got := boundNames(runs, "fix"); !slices.Equal(got, []string{"store-b"}) || runs[0].BindingSource == BindingSourceCredited {
		t.Fatalf("stage runs = %+v, want fix bound to the sibling store-b through the finding", runs)
	}
	// An incident that opened on another alert binds it the same way.
	keyed.OpeningKeys = []string{"another-alert"}
	runs = SelectKeyedProgramCandidates(context.Background(), registry, keyedView("store-b"), findings, time.Second, keyed)
	if got := boundNames(runs, "fix"); !slices.Equal(got, []string{"store-b"}) {
		t.Fatalf("other opening alert: bound %v, want store-b", got)
	}
	// Diagnose stays the gate: a sibling where it does not match is not applied.
	registry = keyedRegistry(t, func(program.Bindings) program.Result { return program.NoMatch }, "fix")
	runs = SelectKeyedProgramCandidates(context.Background(), registry, keyedView("store-b"), findings, time.Second, keyed)
	for _, run := range runs {
		if run.Verdict.Result == program.Match {
			t.Fatalf("NoMatch diagnosis applied: %+v", runs)
		}
	}
}

// A credited suspect ranks before a finding-bound sibling, and the stage keeps
// one Match per playbook per pass.
func TestStageRanksCreditedSuspectBeforeFindingBoundSibling(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix")
	// The sibling comes first in findings order; the credit still ranks first.
	findings := []sdk.Finding{keyedFinding("store-b"), keyedFinding("store-a", "gateway-db")}
	findings[1].Fingerprint = "second"
	keyed := KeyedMatching{Credits: credits("fix", "k1", "store-a"), OpeningKeys: []string{"k1"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("store-a", "store-b", "gateway-db"), findings, time.Second, keyed)
	var matched []string
	for _, run := range runs {
		if run.ProgramID == "fix" && run.Verdict.Result == program.Match {
			matched = append(matched, run.Bindings.Objects[ComponentRole].Name+"/"+run.BindingSource)
		}
	}
	if !slices.Equal(matched, []string{"store-a/" + BindingSourceCredited}) {
		t.Fatalf("matched %v, want only the credited store-a", matched)
	}
}

// Two credited suspects that both Match apply only the first in suspect order.
func TestStageKeepsOnlyTheFirstMatchInSuspectOrder(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix")
	// "zeta" comes first in the finding; sorting by name would pick "alpha".
	findings := []sdk.Finding{keyedFinding("zeta", "alpha")}
	keyed := KeyedMatching{Credits: credits("fix", "k1", "alpha", "zeta"), OpeningKeys: []string{"k1"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("zeta", "alpha"), findings, time.Second, keyed)
	if got := boundNames(runs, "fix"); !slices.Equal(got, []string{"zeta"}) {
		t.Fatalf("bound %v, want only the first suspect zeta", got)
	}
	// A later suspect that does not Match is kept as evidence, and the first Match still wins.
	registry = keyedRegistry(t, func(b program.Bindings) program.Result {
		if b.Objects[ComponentRole].Name == "zeta" {
			return program.NoMatch
		}
		return program.Match
	}, "fix")
	runs = SelectKeyedProgramCandidates(context.Background(), registry, keyedView("zeta", "alpha"), findings, time.Second, keyed)
	matched := []string{}
	for _, run := range runs {
		if run.Verdict.Result == program.Match {
			matched = append(matched, run.Bindings.Objects[ComponentRole].Name)
		}
	}
	if !slices.Equal(matched, []string{"alpha"}) || len(runs) != 2 {
		t.Fatalf("runs = %+v, want zeta NoMatch and alpha Match", runs)
	}
}

// A playbook with no credited pair keeps the finding binding, and a credited
// one binds an uncredited suspect through it too.
func TestPlaybookWithoutPairsKeepsFindingBinding(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "legacy", "fix")
	findings := []sdk.Finding{keyedFinding("api")}
	keyed := KeyedMatching{Credits: credits("fix", "k1", "front"), OpeningKeys: []string{"k1"}, Stage: true}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("api"), findings, time.Second, keyed)
	if got := boundNames(runs, "legacy"); !slices.Equal(got, []string{"api"}) || runs[0].BindingSource == "credited" {
		t.Fatalf("legacy playbook bound %v (%+v), want api by finding binding", got, runs)
	}
	if got := boundNames(runs, "fix"); !slices.Equal(got, []string{"api"}) {
		t.Fatalf("keyed playbook bound %v, want api through the finding", got)
	}
}

func TestCreditedPairWithAnUnbindableRoleSetIsSkipped(t *testing.T) {
	spec := program.ProgramSpec{ID: "fix", Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}, {Name: "store", Kind: "StatefulSet", Required: true}}}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: spec, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	keyed := KeyedMatching{Credits: credits("fix", "k1", "front"), OpeningKeys: []string{"k1"}, Stage: true}
	if runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("front"), []sdk.Finding{keyedFinding("front")}, time.Second, keyed); len(runs) != 0 {
		t.Fatalf("a required parameter the key cannot bind was diagnosed: %+v", runs)
	}
}
