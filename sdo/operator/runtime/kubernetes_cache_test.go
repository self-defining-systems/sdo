package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

func TestKubernetesCacheDeclaresTheMetricsWatchWithoutAnInformer(t *testing.T) {
	_, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset()},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
			ID: "metrics", Interval: time.Second, Watches: []sdk.WatchKind{metrics.Watch},
		}}})
	if err != nil {
		t.Fatalf("a metrics watch must not require an informer: %v", err)
	}
}

func TestKubernetesCacheRoutesSyntheticTrafficNotificationsWithoutAnInformer(t *testing.T) {
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset()},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
			ID: "traffic", Interval: time.Second, Watches: []sdk.WatchKind{traffic.Watch},
		}}})
	if err != nil {
		t.Fatalf("a synthetic watch must not require an informer: %v", err)
	}
	cache.Notify(traffic.Watch)
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: traffic.Watch.APIVersion, Kind: traffic.Watch.Kind, Namespace: "demo"})
}

func TestKubernetesCacheRoutesServiceReachabilityNotificationsWithoutAnInformer(t *testing.T) {
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset()},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
			ID: "service-reachability", Interval: time.Second, Watches: servicehealth.ReachabilityWatches(),
		}}})
	if err != nil {
		t.Fatalf("the reachability watch must not require an informer: %v", err)
	}
	cache.Notify(servicehealth.ReachabilityWatch)
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: servicehealth.ReachabilityWatch.APIVersion, Kind: servicehealth.ReachabilityWatch.Kind, Namespace: "demo"})
}

func TestKubernetesCacheUsesDeclaredInformerUnionWithoutRelisting(t *testing.T) {
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "demo"}, Data: map[string]string{"state": "original"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "undeclared", Namespace: "demo"}},
	)
	var deploymentLists atomic.Int32
	var configMapLists atomic.Int32
	client.PrependReactor("list", "deployments", countList(&deploymentLists))
	client.PrependReactor("list", "configmaps", countList(&configMapLists))

	cache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
	}, []sdk.Detector{
		scheduledDetector{spec: sdk.DetectorSpec{ID: "a", Interval: time.Second, Watches: []sdk.WatchKind{
			{APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "v1", Kind: "ConfigMap"},
		}}},
		scheduledDetector{spec: sdk.DetectorSpec{ID: "b", Interval: time.Second, Watches: []sdk.WatchKind{
			{APIVersion: "v1", Kind: "ConfigMap"},
		}}},
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatalf("wait for sync: %v", err)
	}
	drainWatchEvents(cache)

	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.Deployments()) != 1 || len(snapshot.ConfigMaps()) != 1 || len(snapshot.Services()) != 0 {
		t.Fatalf("snapshot does not reflect declared informer union: %#v", snapshot)
	}
	configMaps := snapshot.ConfigMaps()
	configMaps[0].Data["state"] = "mutated"
	second, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if got := second.ConfigMaps()[0].Data["state"]; got != "original" {
		t.Fatalf("snapshot leaked informer object mutation: %q", got)
	}
	if deploymentLists.Load() != 1 || configMapLists.Load() != 1 {
		t.Fatalf("unexpected API relists: deployments=%d configmaps=%d", deploymentLists.Load(), configMapLists.Load())
	}
}

func TestKubernetesCacheSnapshotsCarryTheirObservationTimeThroughTraffic(t *testing.T) {
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset()},
		[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
			ID: "pods", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	snapshot, err := TrafficSnapshotProvider{Base: cache}.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clock, ok := snapshot.(sdk.ObservationClock)
	if !ok {
		t.Fatalf("traffic snapshot %T must expose its observation time", snapshot)
	}
	if observed := clock.ObservedAt(); observed.Before(before) || observed.After(time.Now()) {
		t.Fatalf("observation time %v outside the snapshot call", observed)
	}
}

func TestKubernetesCacheRoutesEventsAndIgnoresUndeclaredAndInternalState(t *testing.T) {
	client := fake.NewSimpleClientset()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
	}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
		ID: "configmaps", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap"}},
	}}})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatalf("wait for sync: %v", err)
	}

	configMap, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "settings"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create ConfigMap: %v", err)
	}
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"})
	configMap.Data = map[string]string{"updated": "true"}
	if _, err := client.CoreV1().ConfigMaps("demo").Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update ConfigMap: %v", err)
	}
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"})
	if err := client.CoreV1().ConfigMaps("demo").Delete(ctx, "settings", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete ConfigMap: %v", err)
	}
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"})

	if _, err := client.CoreV1().Services("demo").Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "undeclared"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create undeclared Service: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create internal state ConfigMap: %v", err)
	}
	select {
	case <-cache.Notifications():
		t.Fatalf("unexpected event for undeclared/internal resource: %#v", cache.TakeEvents())
	case <-time.After(50 * time.Millisecond):
	}
}

func TestKubernetesCacheWaitForSyncHonorsCancellation(t *testing.T) {
	cache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: fake.NewSimpleClientset(),
	}, []sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{
		ID: "pods", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
	}}})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cache.WaitForSync(ctx); err == nil {
		t.Fatal("expected cancelled sync")
	}
}

func countList(counter *atomic.Int32) func(ktesting.Action) (bool, runtime.Object, error) {
	return func(ktesting.Action) (bool, runtime.Object, error) {
		counter.Add(1)
		return false, nil, nil
	}
}

func awaitWatchEvent(t *testing.T, cache *KubernetesCache, want sdk.WatchKind) {
	t.Helper()
	select {
	case <-cache.Notifications():
		got := cache.TakeEvents()
		if len(got) != 1 || got[0] != want {
			t.Fatalf("unexpected watch events: got %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func drainWatchEvents(cache *KubernetesCache) {
	select {
	case <-cache.Notifications():
	default:
	}
	cache.TakeEvents()
}

func TestKubernetesCacheResyncObservesRecreatedNamespaceAfterStop(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "demo"}})
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: client}, []sdk.Detector{
		scheduledDetector{spec: sdk.DetectorSpec{
			ID: "pods", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		}},
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatalf("wait for sync: %v", err)
	}
	cache.Stop()
	drainWatchEvents(cache)

	// The application namespace is torn down and redeployed while observation is stopped.
	if err := client.CoreV1().Pods("demo").Delete(ctx, "old", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	if _, err := client.CoreV1().Pods("demo").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "new"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if err := cache.Resync(ctx); err != nil {
		t.Fatalf("resync: %v", err)
	}
	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if pods := snapshot.Pods(); len(pods) != 1 || pods[0].Name != "new" {
		t.Fatalf("resynced cache must reflect only the redeployed objects, got %#v", pods)
	}
	drainWatchEvents(cache)
	if _, err := client.CoreV1().Pods("demo").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "later"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create later pod: %v", err)
	}
	awaitWatchEvent(t, cache, sdk.WatchKind{APIVersion: "v1", Kind: "Pod", Namespace: "demo"})
}
