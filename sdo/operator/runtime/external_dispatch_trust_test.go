package runtime

import (
	"context"
	"testing"

	"sdo.dev/operator/memory"
)

type forbiddenDispatchTrust struct{ memory.Reader }

func (forbiddenDispatchTrust) List(context.Context) ([]memory.Outcome, error) {
	panic("external alert scanned Go program trust")
}

func TestExternalDispatchDoesNotScanGoProgramTrust(t *testing.T) {
	trust, err := (KubernetesJobDispatcher{Outcomes: forbiddenDispatchTrust{}}).programTrust(context.Background(), IncidentRequest{ExternalMessage: "investigate"})
	if err != nil || len(trust) != 0 {
		t.Fatalf("trust=%v err=%v", trust, err)
	}
}
