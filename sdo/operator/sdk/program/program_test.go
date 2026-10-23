package program_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
	"sdo.dev/operator/sdk/sdktest"
)

var serviceRef = sdk.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}

func TestValidateBindings(t *testing.T) {
	spec := program.ProgramSpec{Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "pod", Kind: "Pod"}}}
	for _, tc := range []struct {
		name     string
		bindings program.Bindings
		want     string
	}{
		{"valid", program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}, ""},
		{"missing", program.Bindings{Objects: program.ObjectBindings{}}, "service"},
		{"wrong kind", program.Bindings{Objects: program.ObjectBindings{"service": {Kind: "Pod", Name: "front"}}}, "kind"},
		{"unknown", program.Bindings{Objects: program.ObjectBindings{"service": serviceRef, "extra": serviceRef}}, "extra"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := program.ValidateBindings(spec, tc.bindings)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("ValidateBindings() = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestVerdictAndPlanValidation(t *testing.T) {
	for _, verdict := range []program.Verdict{
		{Result: program.Match, Question: "why?"},
		{Result: program.NeedsJudgement},
		{Result: "other"},
	} {
		if err := verdict.Validate(); err == nil {
			t.Errorf("accepted invalid verdict %+v", verdict)
		}
	}
	if err := (program.Verdict{Result: program.NeedsJudgement, Question: "which revision?"}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"spec.ports[0].targetPort", "spec.template.spec.containers[0].image", "spec.replicas", "spec.template.spec.containers[0].env[0].value", "spec.template.spec.containers[0].resources.limits.cpu", "spec.selector.app", "spec.template.spec.containers[0].readinessProbe.httpGet.path"} {
		if err := (program.Plan{Actions: []program.Action{program.SetField{Ref: serviceRef, Path: path, Value: testPlanValue(1)}}}).Validate(); err != nil {
			t.Errorf("rejected %s: %v", path, err)
		}
	}
	for _, path := range []string{"metadata.annotations.x", "spec.portsExtra", "spec.template.spec.containers[0].command", "spec.selectorHack", "spec.ports[0].targetPort.evil"} {
		if err := (program.Plan{Actions: []program.Action{program.SetField{Ref: serviceRef, Path: path}}}).Validate(); err == nil {
			t.Errorf("accepted %s", path)
		}
	}
}

type diagnostic struct {
	fn func(context.Context, program.ReadView, program.Bindings) (program.Verdict, error)
}

type panickingSpec struct{ diagnostic }

func (panickingSpec) Spec() program.ProgramSpec { panic("bad spec") }

func (d diagnostic) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "test", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (d diagnostic) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	return d.fn(ctx, v, b)
}

func TestRunDiagnoseContainsFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fn       func(context.Context, program.ReadView, program.Bindings) (program.Verdict, error)
		bindings program.Bindings
		why      string
	}{
		{"bindings", nil, program.Bindings{Objects: program.ObjectBindings{}}, "service"},
		{"error", func(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
			return program.Verdict{}, errors.New("probe failed")
		}, program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}, "probe failed"},
		{"panic", func(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
			panic("bad program")
		}, program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}, "bad program"},
		{"deadline", func(ctx context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {
			<-ctx.Done()
			return program.Verdict{}, ctx.Err()
		}, program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}, "deadline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := program.RunDiagnose(context.Background(), diagnostic{fn: tc.fn}, &sdktest.ReplayReadView{}, tc.bindings, time.Millisecond)
			if got.Result != program.Unknown || len(got.Evidence) == 0 || !strings.Contains(got.Evidence[0].Observed, tc.why) {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestRunDiagnoseContainsSpecPanic(t *testing.T) {
	got := program.RunDiagnose(context.Background(), panickingSpec{}, &sdktest.ReplayReadView{}, program.Bindings{Objects: program.ObjectBindings{}}, time.Second)
	if got.Result != program.Unknown || len(got.Evidence) == 0 || !strings.Contains(got.Evidence[0].Observed, "bad spec") {
		t.Fatalf("got %+v", got)
	}
}

func TestExampleProgramEndToEnd(t *testing.T) {
	c, err := sdktest.LoadCorpusCase("testdata/.sdo/corpus/targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	p := targetport.Program{}
	sdktest.CheckGeneralization(t, p, c, c.PeerBindings)
	got := program.RunDiagnose(context.Background(), p, &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.Recordings}, c.Bindings, time.Second)
	if got.Result != program.Match {
		t.Fatalf("open verdict: %+v", got)
	}
	locate := func(v sdk.DetectionContext) []program.Bindings { return []program.Bindings{c.Bindings} }
	d := program.DetectorFromProgram(p, locate, sdk.DetectorSpec{ID: p.Spec().ID, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder})
	findings, err := d.Detect(context.Background(), c.Open)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].PrimaryResource != c.Bindings.Objects["service"] || findings[0].Playbooks[0] != p.Spec().ID || findings[0].ParameterBindings["service"] != c.Bindings.Objects["service"] || findings[0].Evidence == "" {
		t.Fatalf("findings: %+v", findings)
	}
	findings, err = d.Detect(context.Background(), c.Fixed)
	if err != nil || len(findings) != 0 {
		t.Fatalf("fixed findings: %+v, %v", findings, err)
	}
}

func TestDetectorCannotUseLiveReadView(t *testing.T) {
	p := diagnostic{fn: func(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
		_, err := v.ProbeTCP(serviceRef, 80)
		return program.Verdict{}, err
	}}
	d := program.DetectorFromProgram(p, func(sdk.DetectionContext) []program.Bindings {
		return []program.Bindings{{Objects: program.ObjectBindings{"service": serviceRef}}}
	}, sdk.DetectorSpec{ID: "test"})
	findings, err := d.Detect(context.Background(), sdktest.Snapshot{})
	if err != nil || len(findings) != 0 {
		t.Fatalf("findings: %+v, %v", findings, err)
	}
}

// Synthetic cases retain the matching/near-miss contract formerly bundled with a learned program.
func TestExampleProgramSyntheticPortMatchesAndNearMisses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target intstr.IntOrString
		noPods bool
		want   program.Result
	}{
		{"numeric match", intstr.FromInt32(8080), false, program.NoMatch},
		{"numeric mismatch", intstr.FromInt32(9999), false, program.Match},
		{"named match", intstr.FromString("web"), false, program.NoMatch},
		{"named mismatch", intstr.FromString("absent"), false, program.Match},
		{"no selected pods", intstr.FromInt32(8080), true, program.Unknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "demo", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "front", Namespace: "demo"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "front"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: tc.target}}}}}}
			if !tc.noPods {
				snapshot.PodList = []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "front-1", Namespace: "demo", Labels: map[string]string{"app": "front"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{Name: "web", ContainerPort: 8080}}}}}}}
			}
			got := program.RunDiagnose(context.Background(), targetport.Program{}, &sdktest.ReplayReadView{Snapshot: snapshot}, program.Bindings{Objects: program.ObjectBindings{"service": serviceRef}}, time.Second)
			if got.Result != tc.want {
				t.Fatalf("got %s, want %s", got.Result, tc.want)
			}
		})
	}
}
