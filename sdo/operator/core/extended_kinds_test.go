package core

import (
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestExtendedWatchKindsValidate(t *testing.T) {
	for _, watch := range []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "StatefulSet"}, {APIVersion: "apps/v1", Kind: "DaemonSet"}, {APIVersion: "batch/v1", Kind: "Job"}, {APIVersion: "batch/v1", Kind: "CronJob"}, {APIVersion: "v1", Kind: "PersistentVolumeClaim"}, {APIVersion: "v1", Kind: "ServiceAccount"}, {APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"}, {APIVersion: "policy/v1", Kind: "PodDisruptionBudget"}, {APIVersion: "networking.k8s.io/v1", Kind: "Ingress"}, {APIVersion: "v1", Kind: "Node"}, {APIVersion: "v1", Kind: "Secret"}} {
		d := validationDetector{spec: sdk.DetectorSpec{ID: watch.Kind, Interval: time.Second, Watches: []sdk.WatchKind{watch}}}
		if err := ValidateDetectors([]sdk.Detector{d}); err != nil {
			t.Fatal(err)
		}
	}
}
