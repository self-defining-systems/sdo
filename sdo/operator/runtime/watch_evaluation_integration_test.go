//go:build integration

package runtime

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"sdo.dev/operator/sdk"
)

func TestKubernetesCacheRetainsObjectPreimageForDrift(t *testing.T) {
	informerCache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}
	next := old.DeepCopy()
	next.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	informerCache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "demo"}).OnUpdate(old, next)
	changes := informerCache.TakeObjectChanges()
	if len(changes) != 1 || changes[0].Old.Object["spec"] == nil || changes[0].New.Object["spec"] == nil {
		t.Fatalf("lost object change: %+v", changes)
	}
	if len(informerCache.TakeObjectChanges()) != 0 {
		t.Fatal("drift change delivered twice")
	}
}

func TestKubernetesCacheRetainsListedObjectBeforeFirstEdit(t *testing.T) {
	informerCache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"}}
	next := old.DeepCopy()
	next.Spec.Ports = []corev1.ServicePort{{Port: 80}}
	handler := informerCache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "demo"})
	handler.OnAdd(old, true)
	handler.OnUpdate(old, next)
	changes := informerCache.TakeObjectChanges()
	if len(changes) != 1 || changes[0].Old == nil || changes[0].New == nil {
		t.Fatalf("first edit lost its listed preimage: %+v", changes)
	}
}

func TestKubernetesCacheDeliversConfigMapDeleteQueuedBehindPodBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"},
		})
		informerCache, err := newCacheForTest(t, KubernetesCacheConfig{
			Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state", ResyncPeriod: time.Hour,
		}, []sdk.Detector{configMapPresenceDetector{spec: healthSpec(podWatch, configMapWatch), configMap: "mongo-geo-script"}})
		if err != nil {
			t.Fatalf("new cache: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		informerCache.Start(ctx)
		if err := informerCache.WaitForSync(ctx); err != nil {
			t.Fatalf("wait for sync: %v", err)
		}
		drainWatchEvents(informerCache)

		// More Pod notifications than the former 256-slot channel held, all before
		// the loop drains any of them, followed by the fault's ConfigMap delete.
		// The fake watcher buffers only 100 events, so the Pod burst is delivered
		// through the informer handler directly.
		podHandler := informerCache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Pod", Namespace: "demo"})
		for index := 0; index < 300; index++ {
			podHandler.OnAdd(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", index), Namespace: "demo"},
			}, false)
		}
		if err := client.CoreV1().ConfigMaps("demo").Delete(ctx, "mongo-geo-script", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete ConfigMap: %v", err)
		}
		awaitConfigMapAbsent(t, informerCache, "mongo-geo-script")

		events := takeEventsThrough(t, informerCache, "Pod", "ConfigMap")
		want := map[sdk.WatchKind]bool{
			{APIVersion: "v1", Kind: "Pod", Namespace: "demo"}:       false,
			{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}: false,
		}
		for _, event := range events {
			want[event] = true
		}
		for kind, seen := range want {
			if !seen {
				t.Fatalf("watch kind %#v was not delivered; got %#v", kind, events)
			}
		}
		if len(events) != 2 {
			t.Fatalf("expected one coalesced event per kind, got %#v", events)
		}
	})
}

func TestKubernetesCacheRoutesDeleteTombstones(t *testing.T) {
	informerCache, err := newCacheForTest(t, KubernetesCacheConfig{
		Namespace: "demo", Client: fake.NewSimpleClientset(), StateConfigMapName: "sdo-controller-state", ResyncPeriod: time.Hour,
	}, []sdk.Detector{configMapPresenceDetector{spec: incidentSpec(configMapWatch), configMap: "settings"}})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	handler := informerCache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"})
	handler.OnDelete(cache.DeletedFinalStateUnknown{
		Key: "demo/sdo-controller-state",
		Obj: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "demo"}},
	})
	if events := informerCache.TakeEvents(); len(events) != 0 {
		t.Fatalf("controller state tombstone was routed: %#v", events)
	}
	handler.OnDelete(cache.DeletedFinalStateUnknown{
		Key: "demo/settings",
		Obj: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "demo"}},
	})
	select {
	case <-informerCache.Notifications():
	default:
		t.Fatal("tombstone delete did not signal the controller loop")
	}
	events := informerCache.TakeEvents()
	if len(events) != 1 || events[0] != (sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}) {
		t.Fatalf("tombstone delete was not routed: %#v", events)
	}
}

// Reproduces the luna-reuse-v2 stage-1 run: a burst of Pod events is already
// queued when the fault deletes a ConfigMap. The health detector watches Pods
// and ConfigMaps; the learned incident detector watches only Deployments and
// ConfigMaps. One runtime-loop iteration must evaluate and record the incident
// detector, and its Firing:2 confirmation must land in the same incident.
func TestControllerEvaluatesConfigMapWatcherAfterDeleteBehindPodBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "demo"}}
		client := fake.NewSimpleClientset(deployment, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"},
		})
		health := configMapPresenceDetector{spec: healthSpec(podWatch, configMapWatch), configMap: "mongo-geo-script"}
		incident := configMapPresenceDetector{spec: incidentSpec(deploymentWatch, configMapWatch), configMap: "mongo-geo-script"}
		detectors := []sdk.Detector{health, incident}
		informerCache, err := newCacheForTest(t, KubernetesCacheConfig{
			Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state", ResyncPeriod: time.Hour,
		}, detectors)
		if err != nil {
			t.Fatalf("new cache: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		informerCache.Start(ctx)
		if err := informerCache.WaitForSync(ctx); err != nil {
			t.Fatalf("wait for sync: %v", err)
		}
		drainWatchEvents(informerCache)
		start := time.Unix(0, 0).UTC()
		config := testControllerConfig()
		config.BatchDebounce = 500 * time.Millisecond
		config.ConfirmationInterval = time.Second
		dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
		controller, err := NewController(config, detectors, informerCache, dispatcher, start)
		if err != nil {
			t.Fatalf("new controller: %v", err)
		}
		if err := controller.Step(ctx, start, nil); err != nil {
			t.Fatalf("baseline step: %v", err)
		}

		for index := 0; index < 50; index++ {
			if _, err := client.CoreV1().Pods("demo").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", index)},
			}, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create pod: %v", err)
			}
		}
		if err := client.CoreV1().ConfigMaps("demo").Delete(ctx, "mongo-geo-script", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete ConfigMap: %v", err)
		}
		awaitConfigMapAbsent(t, informerCache, "mongo-geo-script")

		faultObserved := start.Add(time.Second)
		if err := controller.StepEvents(ctx, faultObserved, takeEventsThrough(t, informerCache, "ConfigMap")); err != nil {
			t.Fatalf("event step: %v", err)
		}
		if got := lastEvaluation(evaluationsState(exportState(t, controller).History), incident.spec.ID); got.Status != DetectorEvaluationFiring ||
			!got.EvaluatedAt.Equal(faultObserved) {
			t.Fatalf("incident detector was not evaluated after the ConfigMap delete: %#v", got)
		}

		// No further watch event arrives; the pending Firing:2 confirmation must
		// not wait for the 30s interval.
		confirmed := faultObserved.Add(config.ConfirmationInterval)
		if next := controller.NextWake(); next.After(confirmed) {
			t.Fatalf("pending confirmation wakes at %s, want no later than %s", next, confirmed)
		}
		if err := controller.Step(ctx, confirmed, nil); err != nil {
			t.Fatalf("confirmation step: %v", err)
		}
		if err := controller.Step(ctx, confirmed.Add(config.BatchDebounce), nil); err != nil {
			t.Fatalf("debounce step: %v", err)
		}
		executePendingEffect(t, controller)
		request := awaitRequest(t, dispatcher.requests)
		if !requestHasDetector(request, incident.spec.ID) || !requestHasDetector(request, health.spec.ID) {
			t.Fatalf("incident did not carry both detectors' findings: %#v", request.Findings)
		}
		if !requestSurfacesPlaybook(request, incidentPlaybook) {
			t.Fatalf("learned playbook was not surfaced: %#v", request.SurfacedPlaybooks)
		}
		if got := lastEvaluation(request.DetectorHistory, incident.spec.ID); got.Status != DetectorEvaluationFiring {
			t.Fatalf("request history lacks the incident detector's firing evaluation: %#v", request.DetectorHistory)
		}
	})
}
