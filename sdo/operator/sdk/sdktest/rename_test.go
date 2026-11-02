package sdktest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
)

func renameFixture(t *testing.T) CorpusCase {
	t.Helper()
	c, err := LoadCorpusCase("../program/testdata/.sdo/corpus/targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCheckRenameInvarianceTargetport(t *testing.T) {
	CheckRenameInvariance(t, targetport.Program{}, renameFixture(t))
}

type fixedNameDiagnoser struct{}

func (fixedNameDiagnoser) Spec() program.ProgramSpec { return targetport.Program{}.Spec() }
func (fixedNameDiagnoser) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	if b.Objects["service"].Name != "front" {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return targetport.Program{}.Diagnose(context.Background(), v, b)
}

type fixedPortDiagnoser struct{}

func (fixedPortDiagnoser) Spec() program.ProgramSpec { return targetport.Program{}.Spec() }
func (fixedPortDiagnoser) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, service := range v.Services() {
		if service.Name == b.Objects["service"].Name && service.Spec.Ports[0].TargetPort == intstr.FromInt32(8087) {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func TestRenameInvarianceRejectsHardcodedValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    program.Diagnoser
		edit func(*CorpusCase)
	}{
		{"service name", fixedNameDiagnoser{}, func(*CorpusCase) {}},
		{"port 8087", fixedPortDiagnoser{}, func(c *CorpusCase) { c.Open.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8087) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := renameFixture(t)
			tc.edit(&c)
			failures := renameInvarianceFailures(tc.p, c)
			if len(failures) == 0 || !strings.Contains(failures[0], "matched only under original names") {
				t.Fatalf("hard-coded %s: %v", tc.name, failures)
			}
		})
	}
}

func TestRenameCorpusCaseKeepsReferencesAndRecordingsAligned(t *testing.T) {
	c := renameFixture(t)
	c.Open.ServiceList[0].OwnerReferences = []metav1.OwnerReference{{Name: "front"}}
	c.Open.PodList[0].Spec.Containers[0].Image = "registry.example/demo:v1"
	c.Open.PodList[0].Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SERVICE", Value: "front"}}
	c.Open.PodList[0].Spec.Containers[0].Ports[0].Name = "web"
	c.Open.PodList[0].Spec.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}}}
	c.Open.ServiceList[0].Spec.Ports[0].Name = "web"
	c.Open.ServiceList[0].Spec.HealthCheckNodePort = 8080
	ref := sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1"}
	logs, _ := json.Marshal([]any{ref, "app", time.Minute})
	probe, _ := json.Marshal([]any{ref, int32(8080)})
	c.Recordings = []PrimitiveCall{
		{Method: "Logs", Arguments: logs, Result: json.RawMessage(`"front listens on 8080"`)},
		{Method: "ProbeTCP", Arguments: probe, Result: json.RawMessage(`{"success":true}`)},
	}
	renamed, err := renameCorpusCase(c)
	if err != nil {
		t.Fatal(err)
	}
	service := renamed.Open.ServiceList[0]
	pod := renamed.Open.PodList[0]
	if service.Name != "front-rx7" || service.OwnerReferences[0].Name != "front-rx7" || service.Spec.Selector["app"] != "front-rx7" || pod.Labels["app"] != "front-rx7" || renamed.Bindings.Objects["service"].Name != "front-rx7" {
		t.Fatalf("resource references diverged: service=%+v pod=%+v bindings=%+v", service, pod, renamed.Bindings)
	}
	container := pod.Spec.Containers[0]
	if container.Name != "app-rx7" || container.Image != "registry.example/demo-rx7:v1-rx7" || container.Env[0].Value != "front-rx7" || container.Ports[0].Name != "web-rx7" {
		t.Fatalf("container was not renamed consistently: %+v", container)
	}
	if service.Spec.Ports[0].Port != 1080 || service.Spec.Ports[0].TargetPort.IntVal != 10999 || container.Ports[0].ContainerPort != 9080 {
		t.Fatalf("ports were not renamed: service=%+v container=%+v", service.Spec.Ports[0], container.Ports[0])
	}
	if service.Spec.HealthCheckNodePort != 9080 || container.LivenessProbe.HTTPGet.Port.IntVal != 9080 {
		t.Fatalf("probe ports were not renamed: service=%+v probe=%+v", service.Spec, container.LivenessProbe)
	}
	replay := ReplayReadView{Snapshot: renamed.Open, Calls: renamed.Recordings}
	gotLogs, err := replay.Logs(sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1-rx7"}, "app-rx7", time.Minute)
	if err != nil || gotLogs != "front-rx7 listens on 9080" {
		t.Fatalf("renamed logs: %q, %v", gotLogs, err)
	}
	gotProbe, err := replay.ProbeTCP(sdk.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1-rx7"}, 9080)
	if err != nil || !gotProbe.Success {
		t.Fatalf("renamed probe: %+v, %v", gotProbe, err)
	}
	if c.Open.ServiceList[0].Name != "front" || c.Open.PodList[0].Spec.Containers[0].Image != "registry.example/demo:v1" {
		t.Fatal("original corpus case was mutated")
	}
}
