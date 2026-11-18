package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type portRecordingProber struct{ ports []int32 }

func (p *portRecordingProber) Probe(_ context.Context, _ sdk.ObjectRef, port int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	p.ports = append(p.ports, port)
	return program.ProbeResult{Success: true, Status: 200}, nil
}

// The controller's view probes HTTP on the Service's first TCP port, the same
// port rule as the memory host: a leading UDP port is never dialed over HTTP.
func TestControllerProbeHTTPUsesTheServicesFirstTCPPort(t *testing.T) {
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "collector", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
		{Port: 5775, Protocol: corev1.ProtocolUDP},
		{Port: 16686},
	}}}
	prober := &portRecordingProber{}
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{service}}), Context: context.Background(), Prober: prober}
	if _, err := view.ProbeHTTP(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "collector"}, program.HTTPProbe{Method: "GET", Path: "/"}); err != nil {
		t.Fatal(err)
	}
	if len(prober.ports) != 1 || prober.ports[0] != 16686 {
		t.Fatalf("probed ports %v, want [16686]", prober.ports)
	}
}
