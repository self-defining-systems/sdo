package generichealth

import (
	"context"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func targetPortSnapshot(target intstr.IntOrString, ports ...core.ContainerPort) sdktest.Snapshot {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	labels := map[string]string{"app": "example"}
	return sdktest.Snapshot{
		NamespaceName:  "ns",
		ObservedAtTime: &now,
		ServiceList: []core.Service{{
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "svc"},
			Spec: core.ServiceSpec{Selector: labels, Ports: []core.ServicePort{{
				Name: "http", Port: 80, TargetPort: target,
			}}},
		}},
		PodList: []core.Pod{{
			ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "pod-1", Labels: labels},
			Spec:       core.PodSpec{Containers: []core.Container{{Name: "main", Ports: ports}}},
			Status:     core.PodStatus{Phase: core.PodRunning},
		}},
	}
}

func targetPortFindings(t *testing.T, s sdktest.Snapshot) int {
	t.Helper()
	findings, err := NewWiring(sdk.DetectorSpec{ID: "wiring"}).Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range findings {
		if f.RuleID == "service-target-port" {
			n++
		}
	}
	return n
}

// containerPort declarations are informational in Kubernetes: a numeric
// target reaches the Pod's port whether or not a container declares it (an
// image may listen on undeclared ports, or declare a port under another
// protocol). Only a named target depends on a declaration, because Kubernetes
// resolves the name from the declared ports; a numeric target that nothing
// listens on is the reachability detector's (it dials the port).
func TestServiceTargetPortJudgesOnlyNamedTargets(t *testing.T) {
	cases := []struct {
		name   string
		target intstr.IntOrString
		ports  []core.ContainerPort
		want   int
	}{
		{"numeric matches declared port", intstr.FromInt32(8080), []core.ContainerPort{{ContainerPort: 8080}}, 0},
		{"numeric matches no declared port", intstr.FromInt32(9999), []core.ContainerPort{{ContainerPort: 8080}}, 0},
		{"numeric matches port of other protocol", intstr.FromInt32(8080), []core.ContainerPort{{ContainerPort: 8080, Protocol: core.ProtocolUDP}}, 0},
		{"unset target defaults to service port", intstr.IntOrString{}, []core.ContainerPort{{ContainerPort: 80}}, 0},
		{"pod declares no ports", intstr.FromInt32(9999), nil, 0},
		{"named matches declared port", intstr.FromString("web"), []core.ContainerPort{{Name: "web", ContainerPort: 8080}}, 0},
		{"named matches no declared port", intstr.FromString("other"), []core.ContainerPort{{Name: "web", ContainerPort: 8080}}, 1},
		{"named matches port of other protocol", intstr.FromString("web"), []core.ContainerPort{{Name: "web", ContainerPort: 8080, Protocol: core.ProtocolUDP}}, 1},
		{"named target with no declared ports", intstr.FromString("web"), nil, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := targetPortFindings(t, targetPortSnapshot(c.target, c.ports...)); got != c.want {
				t.Fatalf("service-target-port findings = %d, want %d", got, c.want)
			}
		})
	}
}
