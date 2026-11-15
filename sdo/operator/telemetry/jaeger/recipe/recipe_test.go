package recipe

import (
	"strings"
	"testing"

	"sdo.dev/operator/telemetry"
)

func TestRecipeIsPinnedLimitedAndReceivesOTLPAndThrift(t *testing.T) {
	objects := New().Build(telemetry.BackendParams{ControlNamespace: "sdo"})
	if len(objects.Deployments) != 1 || len(objects.Services) != 1 || len(objects.ServiceAccounts) != 1 {
		t.Fatalf("objects = %+v", objects)
	}
	container := objects.Deployments[0].Spec.Template.Spec.Containers[0]
	if !strings.Contains(container.Image, "@sha256:") || container.Resources.Limits.Memory().IsZero() {
		t.Fatalf("image %q is not pinned or has no limits", container.Image)
	}
	ports := map[int32]bool{}
	for _, port := range objects.Services[0].Spec.Ports {
		ports[port.Port] = true
	}
	for _, want := range []int32{16686, 4318, 14268} {
		if !ports[want] {
			t.Errorf("service lacks port %d", want)
		}
	}
	if got := OTLPTracesURL("sdo"); got != "http://sdo-jaeger.sdo.svc:4318" {
		t.Errorf("OTLP URL = %q", got)
	}
}
