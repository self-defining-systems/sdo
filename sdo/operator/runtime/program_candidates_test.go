package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type candidateProgram struct {
	spec program.ProgramSpec
	run  func(context.Context, program.Bindings) program.Verdict
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
	if len(runs) != 1 || runs[0].ProgramID != "missing-configmap" || runs[0].Verdict.Result != program.Unknown {
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
