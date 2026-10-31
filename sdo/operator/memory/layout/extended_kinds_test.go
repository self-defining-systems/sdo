package layout

import (
	"testing"
)

func TestExtendedWatchManifestValidation(t *testing.T) {
	for _, watch := range []Watch{{APIVersion: "apps/v1", Kind: "StatefulSet"}, {APIVersion: "apps/v1", Kind: "DaemonSet"}, {APIVersion: "batch/v1", Kind: "Job"}, {APIVersion: "batch/v1", Kind: "CronJob"}, {APIVersion: "v1", Kind: "PersistentVolumeClaim"}, {APIVersion: "v1", Kind: "ServiceAccount"}, {APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"}, {APIVersion: "policy/v1", Kind: "PodDisruptionBudget"}, {APIVersion: "networking.k8s.io/v1", Kind: "Ingress"}, {APIVersion: "v1", Kind: "Node"}, {APIVersion: "v1", Kind: "Secret"}} {
		m := sampleManifest(ClassHealth, "extended")
		m.Watches = []Watch{watch}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
		m.Watches[0].APIVersion = "invalid/v99"
		if err := m.Validate(); err == nil {
			t.Fatal("accepted invalid version")
		}
	}
}
