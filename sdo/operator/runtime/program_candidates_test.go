package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

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
	if len(got) != 1 || got[0].Bindings.Objects["service"].Name != "api" {
		t.Fatalf("unaffected binding diagnosed: %+v", got)
	}
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
		{DetectorID: "drift.targetport", RuleID: "wrong-port", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"service": ref}},
		{DetectorID: "availability", RuleID: "wrong-port", Status: sdk.FindingActive, PrimaryResource: ref},
	}
	runs := SelectProgramCandidates(context.Background(), registry, view, findings, time.Second)
	if len(runs) != 2 || runs[0].FindingFingerprint == runs[1].FindingFingerprint || runs[0].Verdict.Result != program.Match || runs[1].Verdict.Result != program.Match {
		t.Fatalf("one diagnosis lost a finding association: %+v", runs)
	}
}

func TestSelectProgramCandidatesSweepsUnnamedResourceAndDeduplicates(t *testing.T) {
	geo := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "geo"}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
		if b.Objects["service"].Name == geo.Name {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app"}}, {ObjectMeta: metav1.ObjectMeta{Name: "geo", Namespace: "app"}}}}}
	runs := SelectProgramCandidates(context.Background(), registry, view, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "search"}}}, time.Second)
	if len(runs) != 2 || runs[0].Bindings.Objects["service"].Name != geo.Name || runs[0].Verdict.Result != program.Match || runs[0].BindingSource != "sweep" {
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
		if b.Objects["service"].Name == "geo" && b.Objects["deployment"].Name == "geo" {
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
	run := ProgramRun{ProgramID: "targetport", BindingSource: "sweep", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": {Kind: "Service", Namespace: "app", Name: "geo"}}}, Verdict: program.Verdict{Result: program.Match}}
	got := programRunState(programRunContract(run))
	if got.BindingSource != "sweep" || got.Bindings.Objects["service"].Name != "geo" {
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
			if b.Objects["deployment"].Name == "two" {
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
	if len(got) != 2 || got[0].ProgramID != "pair" || got[0].Verdict.Result != program.Match || got[0].Bindings.Objects["deployment"] != second || got[1].Bindings.Objects["deployment"] != first {
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
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Playbooks: []string{"missing-configmap"}}
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
		if run.Bindings.Objects["from"] == run.Bindings.Objects["to"] {
			t.Fatalf("duplicate resource: %+v", run)
		}
	}
}

func TestSweepAllowsUnboundOptionalParameter(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "optional", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "peer", Kind: "Deployment"}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}}
	runs := SelectProgramCandidates(context.Background(), registry, view, nil, time.Second)
	if len(runs) != 1 || runs[0].Verdict.Result != program.Match {
		t.Fatalf("optional binding discarded sweep: %+v", runs)
	}
	if _, bound := runs[0].Bindings.Objects["peer"]; bound {
		t.Fatal("bound absent optional peer")
	}
}

func TestControllerSweepVisitsTwoHundredBindingsAcrossRestarts(t *testing.T) {
	seen := make(map[string]bool)
	var mu sync.Mutex
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "many", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
		mu.Lock()
		seen[b.Objects["service"].Name] = true
		mu.Unlock()
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	services := make([]corev1.Service, 200)
	for i := range services {
		services[i].Name = fmt.Sprintf("service-%03d", i)
		services[i].Namespace = "app"
	}
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: services}}
	start := time.Unix(10, 0).UTC()
	var state *contracts.ControllerState
	for sweep := 0; sweep < 4; sweep++ {
		config := testControllerConfig()
		config.Namespace = "app"
		controller, err := NewController(config, nil, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
		if err != nil {
			t.Fatal(err)
		}
		controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Programs: registry, SweepInterval: time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
		}})
		if sweep > 0 {
			if err := controller.RestoreState(state); err != nil {
				t.Fatal(err)
			}
		}
		if err := controller.StepEvents(context.Background(), start.Add(time.Duration(sweep+1)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
		raw, err := encodeControllerState(exportState(t, controller))
		if err != nil {
			t.Fatal(err)
		}
		state, err = decodeControllerState(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 200 {
		t.Fatalf("four bounded sweeps visited %d of 200 bindings", len(seen))
	}
}

func TestPeriodicSweepReachesServiceSixtyFour(t *testing.T) {
	services := make([]corev1.Service, 65)
	for i := range services {
		services[i].ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("service-%03d", i), Namespace: "app"}
	}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
		if b.Objects["service"].Name == "service-064" {
			return program.Verdict{Result: program.Match}
		}
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: services}}
	cursors := map[string]string{}
	matched := false
	for i := 0; i < 3; i++ {
		runs := SelectProgramCandidatesWithOptions(context.Background(), registry, view, nil, CandidateOptions{Deadline: time.Second, SweepCursors: cursors})
		if len(runs) != 64 {
			t.Fatalf("sweep count: %d", len(runs))
		}
		for _, run := range runs {
			matched = matched || run.Verdict.Result == program.Match
		}
	}
	if !matched {
		t.Fatal("three sweeps never diagnosed faulty service-064")
	}
}

func TestSweepIncludesOptionalPeerAndUnboundAssignment(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "optional", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "peer", Kind: "Deployment"}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}}
	runs := SelectProgramCandidates(context.Background(), registry, view, nil, time.Second)
	if len(runs) != 2 {
		t.Fatalf("want bound and unbound optional peer assignments, got %+v", runs)
	}
	bound, unbound := false, false
	for _, run := range runs {
		if _, ok := run.Bindings.Objects["peer"]; ok {
			bound = true
		} else {
			unbound = true
		}
	}
	if !bound || !unbound {
		t.Fatalf("lost optional assignment: %+v", runs)
	}
}
