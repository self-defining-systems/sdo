package replay

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type fixtureProgram struct {
	mode  string
	wrong bool
}

func (p fixtureProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.mode} }
func (p fixtureProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	bad := false
	switch p.mode {
	case "field":
		for _, s := range v.Services() {
			bad = bad || s.Spec.Selector["app"] != "api"
		}
	case "create":
		bad = len(v.ConfigMaps()) == 0
	case "delete":
		bad = len(v.NetworkPolicies()) != 0
	case "scale":
		for _, d := range v.Deployments() {
			bad = bad || d.Spec.Replicas == nil || *d.Spec.Replicas != 2
		}
	}
	if bad {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p fixtureProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	var a program.Action
	switch p.mode {
	case "field":
		path := "spec.selector.app"
		if p.wrong {
			path = "spec.selector.other"
		}
		a = program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, "spec.selector.app")}
	case "create":
		ref.Kind = "ConfigMap"
		a = program.CreateFromSource{Ref: ref, Reason: "restore synthetic fixture"}
	case "delete":
		ref.Kind = "NetworkPolicy"
		ref.APIVersion = "networking.k8s.io/v1"
		a = program.DeleteObject{Ref: ref, Reason: "remove synthetic fault"}
	case "scale":
		ref.Kind = "Deployment"
		ref.APIVersion = "apps/v1"
		a = program.ScaleWorkload{Ref: ref, Replicas: program.FromSource(ref, "spec.replicas")}
	}
	return program.Plan{Actions: []program.Action{a}}, nil
}
func fixture(mode string) Case {
	meta := metav1.ObjectMeta{Name: "api", Namespace: "app"}
	open := sdktest.Snapshot{NamespaceName: "app"}
	healthy := sdktest.Snapshot{NamespaceName: "app"}
	source := []*unstructured.Unstructured{}
	switch mode {
	case "field":
		s := corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "wrong", "untouched": "keep"}}}
		open.ServiceList = []corev1.Service{s}
		s = *s.DeepCopy()
		s.Spec.Selector["app"] = "api"
		healthy.ServiceList = []corev1.Service{s}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"selector": map[string]any{"app": "api", "untouched": "keep"}}}})
	case "create":
		healthy.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: meta, Data: map[string]string{"setting": "synthetic"}}}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "api", "namespace": "app"}, "data": map[string]any{"setting": "synthetic"}}})
	case "delete":
		open.NetworkPolicyList = []networkingv1.NetworkPolicy{{ObjectMeta: meta}}
	case "scale":
		zero, two := int32(0), int32(2)
		open.DeploymentList = []appsv1.Deployment{{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Replicas: &zero}}}
		healthy.DeploymentList = []appsv1.Deployment{{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{Replicas: &two}}}
		source = append(source, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"replicas": int64(2)}}})
	}
	return Case{ID: mode, Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: healthy, RootCauseProgramIDs: []string{mode}}, Accepted: source}
}
func TestFromSourceSetFieldRepairs(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "replayed" {
		t.Fatalf("%+v", r)
	}
}
func TestWrongPathFixCaught(t *testing.T) {
	p := fixtureProgram{mode: "field", wrong: true}
	r := Check(context.Background(), p, p, nil, fixture("field"))
	if r.Status != "failed" && r.Status != "refused" {
		t.Fatalf("%+v", r)
	}
}
func TestCreateDeleteScaleRepair(t *testing.T) {
	for _, mode := range []string{"create", "delete", "scale"} {
		t.Run(mode, func(t *testing.T) {
			p := fixtureProgram{mode: mode}
			r := Check(context.Background(), p, p, nil, fixture(mode))
			if r.Status != "replayed" {
				t.Fatalf("%+v", r)
			}
		})
	}
}

type broadDetector struct{}

func (broadDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "broad", Playbooks: []string{"field"}}
}
func (broadDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{Status: sdk.FindingActive}}, nil
}
func TestMatrixFlagsOverbroadDetector(t *testing.T) {
	p := fixtureProgram{mode: "field"}
	reg, _ := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	rows := Matrix(context.Background(), reg, []sdk.Detector{broadDetector{}}, []Case{fixture("create")})
	if len(rows) != 1 || len(rows[0].FalsePositives) == 0 {
		t.Fatalf("%+v", rows)
	}
}
