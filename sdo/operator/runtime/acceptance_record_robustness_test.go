package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type adoptionSources []*unstructured.Unstructured

func (s adoptionSources) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	objects := make([]*unstructured.Unstructured, 0, len(s))
	for _, object := range s {
		objects = append(objects, object.DeepCopy())
	}
	return objects, nil
}

func (s adoptionSources) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	for _, object := range s {
		if object.GetKind() == ref.Kind && object.GetName() == ref.Name {
			return object.DeepCopy(), nil
		}
	}
	return nil, errors.New("not declared")
}

func (s adoptionSources) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("no revisions")
}

// A record belongs to the object instance it was taken on: copied onto a
// recreated object it is not an acceptance point.
func TestAcceptanceRecordIsBoundToObjectUID(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "app"}, "data": map[string]any{"mode": "fast"}}}
	accepted := source.DeepCopy()
	accepted.SetUID("first")
	accepted.Object["data"].(map[string]any)["debug"] = "true"
	if err := RecordAcceptance(accepted, source); err != nil {
		t.Fatal(err)
	}
	if added, err := addedSourceFields(accepted, source); err != nil || len(added) != 0 {
		t.Fatalf("record not current on its own object: %v %v", added, err)
	}
	recreated := accepted.DeepCopy()
	recreated.SetUID("second")
	if _, err := addedSourceFields(recreated, source); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("record copied onto a recreated object compared as current: %v", err)
	}
	if acceptanceRecordCurrent(recreated, source) {
		t.Fatal("adoption would keep a record copied from another object")
	}
}

// An object SDO accepted that comes back without its record (deleted and
// recreated from a clean manifest, or the annotation stripped) must not have
// another writer's undeclared fields absorbed when adoption re-stamps it.
func TestLostAcceptanceRecordKeepsOtherManagersAdditions(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	diff := func() (program.Diff, error) {
		snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*api.service.DeepCopy()}}
		return LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}.DiffAgainstSource(ref)
	}
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	if result, err := diff(); err != nil || result.Changed {
		t.Fatalf("adopted Service not accepted: %+v %v", result, err)
	}

	// The controller restarts from its persisted state; the Service is then
	// recreated by another writer with an undeclared annotation.
	restored, err := NewController(controller.config, nil, adoptionSnapshot{api: api}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	restored.ConfigureDrift(*controller.drift)
	if err := restored.RestoreState(exportState(t, controller)); err != nil {
		t.Fatal(err)
	}
	recreated := *driftService(8080)
	recreated.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	recreated.UID, recreated.ResourceVersion = "recreated", "1"
	recreated.Annotations = map[string]string{"example.com/maintenance": "true"}
	recreated.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl-create", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:example.com/maintenance":{}}},"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}}}
	api.service, api.owners = recreated, nil
	if _, err := diff(); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("recreated object compared as accepted: %v", err)
	}
	restored.adoptionNeeded = true
	if err := restored.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !acceptanceRecordCurrent(serviceObject(t, &api.service), source.object) {
		t.Fatal("recreated object was not re-recorded")
	}
	if result, err := diff(); err != nil || !result.Changed {
		t.Fatalf("re-stamp absorbed another manager's undeclared annotation: %+v %v", result, err)
	}
	delete(api.service.Annotations, "example.com/maintenance")
	if result, err := diff(); err != nil || result.Changed {
		t.Fatalf("re-stamp did not accept declared and deployer fields: %+v %v", result, err)
	}
}

func serviceObject(t *testing.T, service *corev1.Service) *unstructured.Unstructured {
	t.Helper()
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(service)
	if err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: content}
	object.SetAPIVersion("v1")
	object.SetKind("Service")
	return object
}

func settledDeployment(name string) *appsv1.Deployment {
	one := int32(1)
	labelsOf := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Generation: 1, UID: types.UID("uid-" + name), ResourceVersion: "1"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labelsOf}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labelsOf}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: name + ":1"}}}}},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
}

func deploymentSource(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": name, "namespace": "app"}, "spec": map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": name, "image": name + ":1"}}}}}}}
}

func fakeAdoptionController(t *testing.T, fakeClient *fake.Clientset, sources adoptionSources) (*Controller, acceptedSourceBridge) {
	t.Helper()
	client := bridgeFake(t, fakeClient)
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", "accepted"
	controller, err := NewController(config, nil, fastloopObjects{client: fakeClient}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: sources, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Source: sources, SourceCommit: "accepted", Client: client}
	}})
	return controller, client
}

func recordedOn(t *testing.T, client acceptedSourceBridge, ref sdk.ObjectRef, source *unstructured.Unstructured) bool {
	t.Helper()
	resource, err := resolveAcceptedResource(client, ref)
	if err != nil {
		t.Fatal(err)
	}
	object, err := resource.get(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return acceptanceRecordCurrent(object, source)
}

// One object's failed record write must not leave the objects after it
// unadopted, and its retries back off instead of running every pass.
func TestAdoptionIsolatesAcceptanceRecordFailure(t *testing.T) {
	ctx := context.Background()
	fakeClient := fake.NewSimpleClientset(settledDeployment("alpha"), settledDeployment("beta"))
	failing := true
	fakeClient.PrependReactor("patch", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		patch := action.(ktesting.PatchAction)
		if failing && patch.GetName() == "alpha" && strings.Contains(string(patch.GetPatch()), acceptedPathsAnnotation) {
			return true, nil, errors.New("metadata.annotations: Too long")
		}
		return false, nil, nil
	})
	sources := adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}
	controller, client := fakeAdoptionController(t, fakeClient, sources)
	alpha := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "alpha"}
	beta := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "beta"}
	controller.adoptionNeeded = true

	now := time.Unix(11, 0).UTC()
	err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"})
	var recordErr *AcceptanceRecordError
	if !errors.As(err, &recordErr) || recordErr.Ref != alpha {
		t.Fatalf("record failure not reported as a typed per-object error: %v", err)
	}
	controller.recordAdoptionError(now, err)
	if !recordedOn(t, client, beta, sources[1]) {
		t.Fatal("record failure on alpha left beta unrecorded")
	}
	first := controller.ownershipAdoption.RetryAt.Sub(now)
	if !controller.adoptionNeeded || first <= 0 {
		t.Fatalf("failed record not retried: %+v", controller.ownershipAdoption)
	}
	now = controller.ownershipAdoption.RetryAt
	err = controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"})
	controller.recordAdoptionError(now, err)
	if second := controller.ownershipAdoption.RetryAt.Sub(now); second <= first || second > time.Minute {
		t.Fatalf("record retries do not back off within a bound: %v then %v", first, second)
	}

	failing = false
	now = controller.ownershipAdoption.RetryAt
	if err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if !recordedOn(t, client, alpha, sources[0]) || controller.adoptionNeeded || controller.ownershipAdoption.LastError != "" {
		t.Fatalf("recovered record write not completed: %+v", controller.ownershipAdoption)
	}
}

// A declared bare Pod the scheduler cannot place stays unsettled
// indefinitely. Adoption keeps waiting for it, but with growing spacing.
func TestAdoptionBacksOffWhileObjectStaysUnsettled(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "app", UID: "uid-worker", ResourceVersion: "1"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "worker:1"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}},
	}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "worker", "namespace": "app"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "worker", "image": "worker:1"}}}}}
	controller, client := fakeAdoptionController(t, fake.NewSimpleClientset(pod), adoptionSources{source})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
	controller.adoptionNeeded = true

	now := time.Unix(11, 0).UTC()
	previous := time.Duration(0)
	for pass := 0; pass < 6; pass++ {
		if err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"}); err != nil {
			t.Fatal(err)
		}
		if recordedOn(t, client, ref, source) || !controller.adoptionNeeded {
			t.Fatal("unscheduled Pod recorded before it settled")
		}
		wait := controller.ownershipAdoption.RetryAt.Sub(now)
		if wait < previous || wait > acceptanceSettleRetryMax {
			t.Fatalf("pass %d waits %v after %v (bound %v)", pass, wait, previous, acceptanceSettleRetryMax)
		}
		previous = wait
		now = controller.ownershipAdoption.RetryAt
	}
	if previous < time.Minute {
		t.Fatalf("unsettled waits stayed short: %v", previous)
	}
}
