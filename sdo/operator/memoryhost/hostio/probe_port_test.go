package hostio

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A Service may list UDP ports before its HTTP one (a tracing collector does).
// An HTTP probe speaks TCP, so it must use the first TCP port, never fail as if
// the Service were absent from the mirror because its first port is UDP.
func TestProbeHTTPUsesTheServicesFirstTCPPort(t *testing.T) {
	// Port 1 on loopback refuses: the probe got past port selection and dialed
	// TCP, which answers as a probe result, not a read error.
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "collector", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "127.0.0.1", Ports: []corev1.ServicePort{
		{Name: "agent", Port: 5775, Protocol: corev1.ProtocolUDP},
		{Name: "http", Port: 1, Protocol: corev1.ProtocolTCP},
	}}}
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{service}}, ReadBackends{})
	got, err := view.ProbeHTTP(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "collector"}, program.HTTPProbe{Method: "GET", Path: "/read"})
	if err != nil || got.Success || !strings.Contains(got.Detail, "127.0.0.1:1") {
		t.Fatalf("ProbeHTTP = %+v, %v; want a TCP dial of the HTTP port", got, err)
	}
}

// A Service with no TCP port names that, not a mirror absence.
func TestProbeHTTPNamesAServiceWithoutATCPPort(t *testing.T) {
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "collector", Namespace: "app"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Ports: []corev1.ServicePort{{Port: 5775, Protocol: corev1.ProtocolUDP}}}}
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{service}}, ReadBackends{})
	_, err := view.ProbeHTTP(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "collector"}, program.HTTPProbe{Method: "GET", Path: "/read"})
	if err == nil || !strings.Contains(err.Error(), "no TCP port") {
		t.Fatalf("ProbeHTTP error = %v; want it to name the missing TCP port", err)
	}
}
