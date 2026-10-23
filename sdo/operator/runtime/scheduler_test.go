package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type scheduledDetector struct{ spec sdk.DetectorSpec }

func (d scheduledDetector) Spec() sdk.DetectorSpec { return d.spec }
func (d scheduledDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

func TestSchedulerHonorsIntervalsAndRoutesWatches(t *testing.T) {
	start := time.Date(2026, 7, 9, 18, 0, 0, 0, time.UTC)
	deployments := scheduledDetector{spec: sdk.DetectorSpec{
		ID: "deployments", Interval: 30 * time.Second,
		Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo"}},
	}}
	configMaps := scheduledDetector{spec: sdk.DetectorSpec{
		ID: "configmaps", Interval: time.Minute,
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}},
	}}
	scheduler := NewScheduler([]sdk.Detector{deployments, configMaps}, start)

	if got := detectorIDs(scheduler.Select(start, nil)); !equalStrings(got, []string{"configmaps", "deployments"}) {
		t.Fatalf("initial detectors should be due, got %v", got)
	}
	if got := detectorIDs(scheduler.Select(start.Add(20*time.Second), nil)); len(got) != 0 {
		t.Fatalf("detectors ran before interval: %v", got)
	}
	if got := detectorIDs(scheduler.Select(start.Add(30*time.Second), nil)); !equalStrings(got, []string{"deployments"}) {
		t.Fatalf("unexpected interval selection %v", got)
	}
	event := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}
	if got := detectorIDs(scheduler.Select(start.Add(35*time.Second), &event)); !equalStrings(got, []string{"configmaps"}) {
		t.Fatalf("watch did not route to ConfigMap detector: %v", got)
	}
	otherNamespace := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "other"}
	if got := detectorIDs(scheduler.Select(start.Add(36*time.Second), &otherNamespace)); len(got) != 0 {
		t.Fatalf("watch crossed namespace boundary: %v", got)
	}
}

func detectorIDs(detectors []sdk.Detector) []string {
	result := make([]string, 0, len(detectors))
	for _, detector := range detectors {
		result = append(result, detector.Spec().ID)
	}
	return result
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestSchedulerDefaultEventNamespaceRemainsApplicationLocal(t *testing.T) {
	watches := []sdk.WatchKind{{APIVersion: "v1", Kind: "Service", Namespace: "demo"}}
	event := sdk.WatchKind{APIVersion: "v1", Kind: "Service"}
	if !watchesEventInNamespace(watches, event, "demo") {
		t.Fatal("synthetic app event did not match local watch")
	}
	watches[0].Namespace = "foreign"
	if watchesEventInNamespace(watches, event, "demo") {
		t.Fatal("synthetic event matched foreign watch")
	}
	webhook := sdk.WatchKind{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration"}
	if !watchesEventInNamespace([]sdk.WatchKind{webhook}, webhook, "demo") {
		t.Fatal("cluster event acquired an app namespace")
	}
}
