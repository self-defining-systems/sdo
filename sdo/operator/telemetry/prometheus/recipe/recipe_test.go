package recipe

import (
	"testing"

	"sdo.dev/operator/telemetry"
)

func TestBuildNamesAndEndpoints(t *testing.T) {
	o := New().Build(telemetry.BackendParams{ApplicationNamespace: "app", ControlNamespace: "ctl"})
	if o.Services[0].Namespace != "ctl" || o.ClusterRoles[0].Name != "sdo-prometheus-ctl" || o.ClusterBindings[0].Subjects[0].Namespace != "ctl" {
		t.Fatalf("recipe objects are not bound to the control namespace: %+v", o)
	}
	if got := QueryURL("ctl"); got != "http://sdo-prometheus.ctl.svc:9090" {
		t.Fatal(got)
	}
	if got := OTLPMetricsURL("ctl"); got != "http://sdo-prometheus.ctl.svc:9090/api/v1/otlp/v1/metrics" {
		t.Fatal(got)
	}
}
