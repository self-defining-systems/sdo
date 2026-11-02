package replay

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type portRemovalProgram struct{ wrong bool }

func (portRemovalProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "remove-port"} }
func (portRemovalProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	for _, s := range v.Services() {
		for _, p := range s.Spec.Ports {
			if p.Name == "unused" {
				return program.Verdict{Result: program.Match}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p portRemovalProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	name := "unused"
	if p.wrong {
		name = "http"
	}
	return program.Plan{Actions: []program.Action{program.RemoveServicePort{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "discovery"}, Name: name}}}, nil
}
func TestRemoveServicePortClosedLoop(t *testing.T) {
	meta := metav1.ObjectMeta{Name: "discovery", Namespace: "app"}
	healthy := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 8500}}}}}}
	open := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*healthy.ServiceList[0].DeepCopy()}}
	open.ServiceList[0].Spec.Ports = append(open.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "unused", Port: 8300})
	c := Case{ID: "synthetic-service-port", Corpus: sdktest.CorpusCase{Open: open, Healthy: healthy, Fixed: open, RootCauseProgramIDs: []string{"remove-port"}}}
	for _, wrong := range []bool{false, true} {
		p := portRemovalProgram{wrong}
		r := Check(context.Background(), p, p, nil, c)
		if (r.Status == "replayed") == wrong {
			t.Fatalf("wrong=%v: %+v", wrong, r)
		}
	}
}
