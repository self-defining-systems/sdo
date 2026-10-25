package runtime

import (
	"context"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
)

type missingConfigMapDetector struct{}

func (missingConfigMapDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "missing-configmap", Interval: time.Second,
		Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "v1", Kind: "ConfigMap"}},
	}
}

func (missingConfigMapDetector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	existing := make(map[string]struct{})
	for _, configMap := range snapshot.ConfigMaps() {
		existing[configMap.Namespace+"/"+configMap.Name] = struct{}{}
	}
	findings := make([]sdk.Finding, 0)
	for _, deployment := range snapshot.Deployments() {
		for _, reference := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if reference.Optional {
				continue
			}
			if _, ok := existing[deployment.Namespace+"/"+reference.Name]; ok {
				continue
			}
			deploymentRef := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			configMapRef := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: deployment.Namespace, Name: reference.Name}
			findings = append(findings, sdk.Finding{
				RuleID: "missing-configmap", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
				Summary: "required ConfigMap is absent", Evidence: deployment.Name + " requires " + reference.Name,
				PrimaryResource: deploymentRef, RelatedResources: []sdk.ObjectRef{configMapRef},
				ParameterBindings: map[string]sdk.ObjectRef{"target_resource": deploymentRef, "missing_config_map": configMapRef},
				Fingerprint:       deployment.Namespace + "/" + deployment.Name + "/" + reference.Name,
			})
		}
	}
	return findings, nil
}

func TestControllerIntegrationInformerPersistenceAndRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	required := false
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "renamed-api", Namespace: "demo"},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", EnvFrom: []corev1.EnvFromSource{{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "renamed-settings"}, Optional: &required,
					},
				}}}},
			}}},
		},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "renamed-settings", Namespace: "demo"}},
	)
	detector := missingConfigMapDetector{}
	cache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
	}, []sdk.Detector{detector})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatalf("wait for sync: %v", err)
	}
	drainWatchEvents(cache)
	start := time.Unix(0, 0)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{detector}, cache, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state")
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("attach store: %v", err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatalf("healthy step: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)

	if err := client.CoreV1().ConfigMaps("demo").Delete(ctx, "renamed-settings", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete ConfigMap: %v", err)
	}
	events := awaitEventValue(t, cache)
	if err := controller.StepEvents(ctx, start.Add(100*time.Millisecond), events); err != nil {
		t.Fatalf("first firing: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if err := controller.Step(ctx, start.Add(time.Second+100*time.Millisecond), nil); err != nil {
		t.Fatalf("persistent firing: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist pending dispatch: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if got := request.Findings[0].ParameterBindings["missing_config_map"].Name; got != "renamed-settings" {
		t.Fatalf("unexpected parameter binding %q", got)
	}
	if len(request.DetectorHistory) < 3 {
		t.Fatalf("missing detector history: %#v", request.DetectorHistory)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist incident: %v", err)
	}

	restoredDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(testControllerConfig(), []sdk.Detector{detector}, cache, restoredDispatcher, start)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("restore persisted state: %v", err)
	}
	replayed, ok := restored.PendingDispatchEffect()
	if !ok || replayed.Request.IncidentID != request.IncidentID {
		t.Fatalf("restart did not recover same dispatch effect: %#v", replayed)
	}
	if err := restored.ExecuteDispatchEffect(ctx, replayed); err != nil {
		t.Fatalf("reattach dispatch effect: %v", err)
	}
	replayedRequest := awaitRequest(t, restoredDispatcher.requests)
	if replayedRequest.IncidentID != request.IncidentID {
		t.Fatalf("replayed incident id changed: %q", replayedRequest.IncidentID)
	}
	if err := restored.Step(ctx, start.Add(2*time.Second+100*time.Millisecond), nil); err != nil {
		t.Fatalf("restored firing: %v", err)
	}
	assertNoRequest(t, restoredDispatcher.requests)

	if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "renamed-settings"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("restore ConfigMap: %v", err)
	}
	clearEvents := awaitEventValue(t, cache)
	if err := restored.StepEvents(ctx, start.Add(2200*time.Millisecond), clearEvents); err != nil {
		t.Fatalf("first clear: %v", err)
	}
	if err := restored.Step(ctx, start.Add(3200*time.Millisecond), nil); err != nil {
		t.Fatalf("second clear: %v", err)
	}
	if restored.IncidentOpen() {
		t.Fatal("restored incident did not close after two healthy samples")
	}
}

func awaitEventValue(t *testing.T, cache *KubernetesCache) []sdk.WatchKind {
	t.Helper()
	select {
	case <-cache.Notifications():
		return cache.TakeEvents()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for informer event")
		return nil
	}
}
