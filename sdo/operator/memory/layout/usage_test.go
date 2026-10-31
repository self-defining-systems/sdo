package layout

import "testing"

func TestManifestUsageWatch(t *testing.T) {
	m := sampleManifest(ClassIncident, "usage")
	m.Watches = []Watch{{APIVersion: "sdo.dev/v1alpha1", Kind: "ResourceUsage"}}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestManifestRejectsUnknownWatch(t *testing.T) {
	m := sampleManifest(ClassIncident, "usage")
	m.Watches = []Watch{{APIVersion: "v1", Kind: "Unknown"}}
	if err := m.Validate(); err == nil {
		t.Fatal("unknown watch accepted")
	}
}
