package layout

import "testing"

func TestLogsWatchKindRegistered(t *testing.T) {
	m := sampleManifest(ClassHealth, "logs")
	m.Watches = []Watch{{APIVersion: "sdo.dev/v1alpha1", Kind: "Logs"}}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	m.Watches[0].APIVersion = "invalid/v99"
	if m.Validate() == nil {
		t.Fatal("unsupported log watch accepted")
	}
}
