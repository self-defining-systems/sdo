package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// failingInformerClient lets each resource's first list (the access probe)
// succeed and fails every later list, so informers start and never sync.
func failingInformerClient(probesSucceed bool) *fake.Clientset {
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	seen := map[string]bool{}
	client.PrependReactor("list", "*", func(action clienttesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		resource := action.GetResource().String()
		if probesSucceed && !seen[resource] {
			seen[resource] = true
			return false, nil, nil
		}
		return true, nil, errors.New("list unavailable")
	})
	return client
}

// A failed informer sync, retried or final, must not leave informer goroutines behind.
func TestStateTrackerFailedSyncLeavesNoInformerGoroutines(t *testing.T) {
	requireNoInformerGoroutines(t, "an earlier test")
	for attempt := 0; attempt < 3; attempt++ {
		// A fresh client each try: only its first list per resource (the probe) succeeds.
		tracker := NewStateTracker(StateTrackerConfig{Client: failingInformerClient(true), Namespace: hotel})
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		err := tracker.Start(ctx)
		cancel()
		if err == nil {
			t.Fatal("Start succeeded although no informer could list")
		}
		requireNoInformerGoroutines(t, "a failed StateTracker.Start")
	}
}

func TestStateTrackerStopAwaitsInformerGoroutines(t *testing.T) {
	requireNoInformerGoroutines(t, "an earlier test")
	tracker := NewStateTracker(StateTrackerConfig{Client: fake.NewSimpleClientset(hotelObjects()...), Namespace: hotel})
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	tracker.Stop()
	requireNoInformerGoroutines(t, "StateTracker.Stop")
}

func TestKubernetesCacheFailedResyncAndStopLeaveNoInformerGoroutines(t *testing.T) {
	requireNoInformerGoroutines(t, "an earlier test")
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: failingInformerClient(false)},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "svc", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	// Resync replaces the previous generation each retry; only the live one may run.
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		err := cache.Resync(ctx)
		cancel()
		var syncErr *InformerSyncError
		if !errors.As(err, &syncErr) {
			t.Fatalf("Resync = %v, want an InformerSyncError", err)
		}
	}
	cache.Stop()
	requireNoInformerGoroutines(t, "KubernetesCache.Stop after failed Resync retries")
}
