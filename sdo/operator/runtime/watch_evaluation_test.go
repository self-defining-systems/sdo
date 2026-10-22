package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const (
	incidentPlaybook = "missing-init-configmap"
	healthPlaybook   = "health-objective"
)

// configMapPresenceDetector fires while the named ConfigMap is absent from the
// snapshot, like the learned missing-geo-mongo-init-configmap detector.
type configMapPresenceDetector struct {
	spec      sdk.DetectorSpec
	configMap string
}

func (d configMapPresenceDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d configMapPresenceDetector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	for _, configMap := range snapshot.ConfigMaps() {
		if configMap.Name == d.configMap {
			return nil, nil
		}
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: snapshot.Namespace(), Name: d.configMap}
	return []sdk.Finding{{
		RuleID: "required-configmap-missing", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "required ConfigMap is absent", Evidence: d.configMap + " is absent", PrimaryResource: ref,
		Fingerprint: d.spec.ID + "/" + d.configMap,
	}}, nil
}

func healthSpec(watches ...sdk.WatchKind) sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "health-objective", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: watches, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{healthPlaybook}, OriginatingCommit: "bootstrap",
	}
}

func incidentSpec(watches ...sdk.WatchKind) sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "missing-init-configmap", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Watches: watches, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{incidentPlaybook}, OriginatingCommit: "learned", OriginatingIncident: "demo-1",
	}
}

var (
	podWatch        = sdk.WatchKind{APIVersion: "v1", Kind: "Pod"}
	configMapWatch  = sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap"}
	deploymentWatch = sdk.WatchKind{APIVersion: "apps/v1", Kind: "Deployment"}
)

func TestKubernetesCacheRetainsObjectPreimageForDrift(t *testing.T) {
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
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
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
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
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"},
	})
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
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
}

func TestKubernetesCacheRoutesDeleteTombstones(t *testing.T) {
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: fake.NewSimpleClientset(), StateConfigMapName: "sdo-controller-state",
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
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "demo"}}
	client := fake.NewSimpleClientset(deployment, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"},
	})
	health := configMapPresenceDetector{spec: healthSpec(podWatch, configMapWatch), configMap: "mongo-geo-script"}
	incident := configMapPresenceDetector{spec: incidentSpec(deploymentWatch, configMapWatch), configMap: "mongo-geo-script"}
	detectors := []sdk.Detector{health, incident}
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
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
}

// The health detector opens the incident; the incident detector confirms after
// the batch drained but before the responder launched.
func TestIncidentFindingActivatedBeforeResponderLaunchIsAttached(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	health := controllerDetector("health-objective", time.Hour, criticalFinding("health"))
	health.spec = healthSpec(podWatch)
	health.spec.Persistence.Firing = 1
	health.spec.Batching.Debounce = 0
	incident := controllerDetector("missing-init-configmap", 30*time.Second,
		sdk.Finding{}, criticalFinding("configmap"))
	incident.spec = incidentSpec(configMapWatch)
	config := testControllerConfig()
	config.ConfirmationInterval = time.Second
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{health, incident},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("health activation: %v", err)
	}
	if !controller.IncidentOpen() {
		t.Fatal("health activation did not open an incident")
	}
	opened := exportState(t, controller).IncidentRequest.IncidentId

	event := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}
	if err := controller.StepEvents(context.Background(), start.Add(100*time.Millisecond), []sdk.WatchKind{event}); err != nil {
		t.Fatalf("first incident firing: %v", err)
	}
	if err := controller.Step(context.Background(), start.Add(1100*time.Millisecond), nil); err != nil {
		t.Fatalf("incident confirmation: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if request.IncidentID != opened {
		t.Fatalf("attachment opened a different incident: %q != %q", request.IncidentID, opened)
	}
	if !requestHasDetector(request, incident.spec.ID) {
		t.Fatalf("incident finding was not attached before launch: %#v", request.Findings)
	}
	if !requestSurfacesPlaybook(request, incidentPlaybook) || !requestSurfacesPlaybook(request, healthPlaybook) {
		t.Fatalf("attached finding's playbook was not surfaced: %#v", request.SurfacedPlaybooks)
	}
	if got := lastEvaluation(request.DetectorHistory, incident.spec.ID); got.Status != DetectorEvaluationFiring {
		t.Fatalf("request history was not refreshed on attachment: %#v", request.DetectorHistory)
	}
	if keys := exportState(t, controller).IncidentFindingKeys; len(keys) != 2 {
		t.Fatalf("attached finding is not tracked for verification: %#v", keys)
	}
}

// The health batch is still debouncing when the incident detector confirms.
func TestIncidentFindingActivatedWithinHealthDebounceJoinsDispatch(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	health := controllerDetector("health-objective", time.Hour, criticalFinding("health"))
	health.spec = healthSpec(podWatch)
	health.spec.Persistence.Firing = 1
	incident := controllerDetector("missing-init-configmap", 30*time.Second,
		sdk.Finding{}, criticalFinding("configmap"))
	incident.spec = incidentSpec(configMapWatch)
	incident.spec.Persistence.Firing = 1
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{health, incident},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("health activation: %v", err)
	}
	event := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}
	if err := controller.StepEvents(context.Background(), start.Add(300*time.Millisecond), []sdk.WatchKind{event}); err != nil {
		t.Fatalf("incident activation: %v", err)
	}
	if controller.IncidentOpen() {
		t.Fatal("incident opened before the health debounce elapsed")
	}
	if err := controller.Step(context.Background(), start.Add(800*time.Millisecond), nil); err != nil {
		t.Fatalf("debounce deadline: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if !requestHasDetector(request, incident.spec.ID) || !requestHasDetector(request, health.spec.ID) {
		t.Fatalf("findings from the debounce window were not dispatched together: %#v", request.Findings)
	}
}

func TestLaunchedResponderKeepsRequestAndBatchDoesNotSpinLoop(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	health := controllerDetector("health-objective", time.Hour, criticalFinding("health"))
	health.spec = healthSpec(podWatch)
	health.spec.Persistence.Firing = 1
	health.spec.Batching.Debounce = 0
	incident := controllerDetector("missing-init-configmap", time.Hour, sdk.Finding{}, criticalFinding("configmap"))
	incident.spec = incidentSpec(configMapWatch)
	incident.spec.Interval = time.Hour
	incident.spec.Persistence.Firing = 1
	incident.spec.Batching.Debounce = 0
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(dispatcher.release)
	controller, err := NewController(testControllerConfig(), []sdk.Detector{health, incident},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("health activation: %v", err)
	}
	executePendingEffect(t, controller)
	launched := awaitRequest(t, dispatcher.requests)

	event := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}
	now := start.Add(time.Second)
	if err := controller.StepEvents(context.Background(), now, []sdk.WatchKind{event}); err != nil {
		t.Fatalf("incident activation: %v", err)
	}
	state := exportState(t, controller)
	if requestHasDetector(*incidentRequestState(state.IncidentRequest), incident.spec.ID) || len(state.IncidentRequest.Findings) != len(launched.Findings) {
		t.Fatalf("launched responder's request was mutated: %#v", state.IncidentRequest.Findings)
	}
	if next := controller.NextWake(); !next.After(now) {
		t.Fatalf("buffered finding that cannot dispatch made the loop wake immediately: %s", next)
	}
}

func criticalFinding(rule string) sdk.Finding {
	return sdk.Finding{
		RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: rule + " fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Name: rule},
		Fingerprint:     rule,
	}
}

func awaitConfigMapAbsent(t *testing.T, informerCache *KubernetesCache, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := informerCache.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		absent := true
		for _, configMap := range snapshot.ConfigMaps() {
			if configMap.Name == name {
				absent = false
			}
		}
		if absent {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("informer never observed ConfigMap %q deletion", name)
}

func lastEvaluation(history []DetectorEvaluation, detectorID string) DetectorEvaluation {
	for index := len(history) - 1; index >= 0; index-- {
		if history[index].DetectorID == detectorID {
			return history[index]
		}
	}
	return DetectorEvaluation{}
}

func requestHasDetector(request IncidentRequest, detectorID string) bool {
	for _, finding := range request.Findings {
		if finding.DetectorID == detectorID {
			return true
		}
	}
	return false
}

func requestSurfacesPlaybook(request IncidentRequest, path string) bool {
	id, err := layout.PlaybookID(path)
	if err != nil {
		return false
	}
	for _, playbook := range request.SurfacedPlaybooks {
		if playbook.PlaybookID == id {
			return true
		}
	}
	return false
}

// takeEventsThrough drains the cache's pending watch events until each of kinds
// has been delivered. An informer updates its store before it notifies its
// handlers, so a store that already reflects a change does not mean the
// matching event is pending yet; under load (e.g. -race in CI) it can lag.
func takeEventsThrough(t *testing.T, informerCache *KubernetesCache, kinds ...string) []sdk.WatchKind {
	t.Helper()
	missing := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		missing[kind] = true
	}
	var events []sdk.WatchKind
	seen := make(map[sdk.WatchKind]bool)
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, event := range informerCache.TakeEvents() {
			if !seen[event] {
				seen[event] = true
				events = append(events, event)
			}
			delete(missing, event.Kind)
		}
		if len(missing) == 0 {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch events for %v were never delivered; got %v", missing, events)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
