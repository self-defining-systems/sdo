package runtime

import (
	"context"
	"testing"
	"time"

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

func TestSelectProgramCandidatesPreservesFindingAssociationsForOneBinding(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.Match}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}}
	findings := []sdk.Finding{
		{DetectorID: "incident-targetport", RuleID: "wrong-port", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"service": ref}},
		{DetectorID: "availability", RuleID: "wrong-port", Status: sdk.FindingActive, PrimaryResource: ref},
	}
	runs := SelectProgramCandidates(context.Background(), registry, view, findings, time.Second)
	if len(runs) != 2 || runs[0].FindingFingerprint == runs[1].FindingFingerprint || runs[0].Verdict.Result != program.Match || runs[1].Verdict.Result != program.Match {
		t.Fatalf("one diagnosis lost a finding association: %+v", runs)
	}
}

func TestProgramRunBindingSourceSurvivesContractRoundTrip(t *testing.T) {
	run := ProgramRun{ProgramID: "targetport", BindingSource: "finding", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": {Kind: "Service", Namespace: "app", Name: "geo"}}}, Verdict: program.Verdict{Result: program.Match}}
	got := programRunState(programRunContract(run))
	if got.BindingSource != "finding" || got.Bindings.Objects["service"].Name != "geo" {
		t.Fatalf("contract round trip: %+v", got)
	}
}

func (p candidateProgram) Spec() program.ProgramSpec { return p.spec }
func (p candidateProgram) Diagnose(ctx context.Context, _ program.ReadView, b program.Bindings) (program.Verdict, error) {
	return p.run(ctx, b), nil
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
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Playbooks: []string{"missing-configmap"}}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{finding}, time.Second)
	if len(runs) != 1 || runs[0].ProgramID != "missing-configmap" || runs[0].BindingSource != "hint" || runs[0].Verdict.Result != program.Unknown {
		t.Fatalf("explicit hint: %+v", runs)
	}
}
