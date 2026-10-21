package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type candidateProgram struct {
	spec program.ProgramSpec
	run  func(context.Context, program.Bindings) program.Verdict
}

func TestSelectProgramCandidatesOnlyDiagnosesAffectedBindings(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.Match}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}, {ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}}}}}
	got := SelectProgramCandidatesWithOptions(context.Background(), registry, view, nil, CandidateOptions{Deadline: time.Second, Affected: []sdk.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}}})
	if len(got) != 1 || got[0].Bindings["service"].Name != "api" {
		t.Fatalf("unaffected binding diagnosed: %+v", got)
	}
}

func TestSelectProgramCandidatesSweepsUnnamedResourceAndDeduplicates(t *testing.T) {
	geo := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "geo"}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
		if b["service"].Name == geo.Name {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app"}}, {ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: "app"}}}}}
	runs := SelectProgramCandidates(context.Background(), registry, view, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "search"}}}, time.Second)
	if len(runs) != 2 || runs[0].Bindings["service"].Name != geo.Name || runs[0].Verdict.Result != program.Match || runs[0].BindingSource != "sweep" {
		t.Fatalf("sweep and rank: %+v", runs)
	}
	if runs[1].BindingSource != "finding" {
		t.Fatalf("finding binding lost: %+v", runs[1])
	}
}

func TestSelectProgramCandidatesSweepLimitAndDeadline(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "slow", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(ctx context.Context, _ program.Bindings) program.Verdict {
		<-ctx.Done()
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	services := make([]corev1.Service, 100)
	for i := range services {
		services[i].Name = fmt.Sprintf("service-%03d", i)
		services[i].Namespace = "app"
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: services}}
	start := time.Now()
	runs := SelectProgramCandidatesWithOptions(context.Background(), registry, view, nil, CandidateOptions{Deadline: 20 * time.Millisecond, SweepLimit: 4})
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("deadline exceeded: %s", elapsed)
	}
	if len(runs) != 4 {
		t.Fatalf("sweep limit: %d", len(runs))
	}
	for _, run := range runs {
		if run.Verdict.Result != program.Unknown || run.BindingSource != "sweep" {
			t.Fatalf("deadline result: %+v", run)
		}
	}
}

func TestSelectProgramCandidatesSweepsRequiredKindPair(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "pair", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
		if b["service"].Name == "geo" && b["deployment"].Name == "geo" {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: "app"}}}, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: "app"}}}}}
	runs := SelectProgramCandidatesWithOptions(context.Background(), registry, view, nil, CandidateOptions{Deadline: time.Second, SweepLimit: 4})
	if len(runs) != 1 || runs[0].Verdict.Result != program.Match || runs[0].BindingSource != "sweep" {
		t.Fatalf("required-kind pair: %+v", runs)
	}
}

func TestProgramRunBindingSourceSurvivesContractRoundTrip(t *testing.T) {
	run := ProgramRun{ProgramID: "targetport", BindingSource: "sweep", Bindings: program.Bindings{"service": {Kind: "Service", Namespace: "app", Name: "geo"}}, Verdict: program.Verdict{Result: program.Match}}
	got := programRunState(programRunContract(run))
	if got.BindingSource != "sweep" || got.Bindings["service"].Name != "geo" {
		t.Fatalf("contract round trip: %+v", got)
	}
}

func (p candidateProgram) Spec() program.ProgramSpec { return p.spec }
func (p candidateProgram) Diagnose(ctx context.Context, _ program.ReadView, b program.Bindings) (program.Verdict, error) {
	return p.run(ctx, b), nil
}

func TestSelectProgramCandidatesBindsEveryResourceAssignmentAndRanks(t *testing.T) {
	service := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "front"}
	first := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "one"}
	second := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "two"}
	registry, err := program.NewRegistry([]program.Diagnoser{
		candidateProgram{spec: program.ProgramSpec{ID: "pair", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "deployment", Kind: "Deployment", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
			if b["deployment"].Name == "two" {
				return program.Verdict{Result: program.Match}
			}
			return program.Verdict{Result: program.NoMatch}
		}},
		candidateProgram{spec: program.ProgramSpec{ID: "missing", Params: []program.ParamSpec{{Name: "config", Kind: "ConfigMap", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: service, RelatedResources: []sdk.ObjectRef{first, second}}}, time.Second)
	if len(got) != 2 || got[0].ProgramID != "pair" || got[0].Verdict.Result != program.Match || got[0].Bindings["deployment"] != second || got[1].Bindings["deployment"] != first {
		t.Fatalf("candidate assignments and rank: %#v", got)
	}
}

func TestSelectProgramCandidatesUsesOneDeadlineForSlowPrograms(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{
		candidateProgram{spec: program.ProgramSpec{ID: "slow"}, run: func(ctx context.Context, _ program.Bindings) program.Verdict {
			<-ctx.Done()
			return program.Verdict{Result: program.NoMatch}
		}},
		candidateProgram{spec: program.ProgramSpec{ID: "fast"}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	got := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{{Status: sdk.FindingActive}}, 20*time.Millisecond)
	if time.Since(started) > 150*time.Millisecond || len(got) != 2 || got[0].Verdict.Result != program.Match || got[1].Verdict.Result != program.Unknown {
		t.Fatalf("shared deadline: %#v, elapsed %s", got, time.Since(started))
	}
}

func TestSelectProgramCandidatesKeepsExplicitHintWithoutBindableKind(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "missing-configmap", Params: []program.ParamSpec{{Name: "config", Kind: "ConfigMap", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Playbooks: []string{".sdo/playbooks/missing-configmap/README.md"}}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{finding}, time.Second)
	if len(runs) != 1 || runs[0].ProgramID != "missing-configmap" || runs[0].BindingSource != "hint" || runs[0].Verdict.Result != program.Unknown {
		t.Fatalf("explicit hint: %+v", runs)
	}
}

func TestSelectProgramCandidatesUsesDistinctResourcesForSameKindRoles(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "link", Params: []program.ParamSpec{{Name: "from", Kind: "Service", Required: true}, {Name: "to", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "a"}
	b := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "b"}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: a, RelatedResources: []sdk.ObjectRef{b}}}, time.Second)
	if len(runs) != 2 {
		t.Fatalf("distinct assignments: %+v", runs)
	}
	for _, run := range runs {
		if run.Bindings["from"] == run.Bindings["to"] {
			t.Fatalf("duplicate resource: %+v", run)
		}
	}
}
