package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
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
		return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}.DiffAgainstSource(ref)
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

// A recreate that carries the earlier instance's record (an installer
// re-applying the object with its annotations) keeps that acceptance point:
// the record follows the live UID. Fields it accepted, still at their accepted
// values, stay accepted even though the recreating writer now owns them (the
// creating request owns every server default). A changed value and a field
// the record never had stay differences. hotel rerun17: two Deployments
// reported 11 server-default paths as residual forever.
func TestRecreatedObjectKeepsItsCarriedAcceptanceRecord(t *testing.T) {
	for _, tc := range []struct {
		name     string
		affinity corev1.ServiceAffinity
		stripped bool
		changed  bool
	}{
		{"defaults unchanged", corev1.ServiceAffinityNone, false, false},
		{"default changed meanwhile", corev1.ServiceAffinityClientIP, false, true},
		// No earlier record to carry: defaults are decided by value against
		// the server's render of accepted source.
		{"stripped record, default value", corev1.ServiceAffinityNone, true, false},
		{"stripped record, writer value", corev1.ServiceAffinityClientIP, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller, api, source, closeServer := adoptionController(t, 8080)
			defer closeServer()
			ctx := context.Background()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			diff := func() (program.Diff, error) {
				snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*api.service.DeepCopy()}}
				return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}.DiffAgainstSource(ref)
			}
			// Server defaults present when SDO first accepts the Service.
			api.service.Spec.Type, api.service.Spec.SessionAffinity = corev1.ServiceTypeClusterIP, corev1.ServiceAffinityNone
			start := time.Unix(10, 0).UTC()
			if err := controller.StepEvents(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			if result, err := diff(); err != nil || result.Changed {
				t.Fatalf("adopted Service not accepted: %+v %v", result, err)
			}
			carried := api.service.Annotations[acceptedPathsAnnotation]
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
			recreated.Spec.Type, recreated.Spec.SessionAffinity = corev1.ServiceTypeClusterIP, tc.affinity
			recreated.Annotations = map[string]string{acceptedPathsAnnotation: carried, "example.com/maintenance": "true"}
			recreated.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl-client-side-apply", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:annotations":{"f:example.com/maintenance":{},"f:sdo.dev/accepted-paths":{}}},"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}},"f:sessionAffinity":{},"f:type":{}}}`)}}}
			if tc.stripped {
				delete(recreated.Annotations, acceptedPathsAnnotation)
			}
			api.service, api.owners = recreated, nil
			if _, err := diff(); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
				t.Fatalf("a record of another instance compared as current: %v", err)
			}
			restored.adoptionNeeded = true
			if err := restored.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if !acceptanceRecordCurrent(serviceObject(t, &api.service), source.object) {
				t.Fatal("recreated object was not re-recorded")
			}
			if result, err := diff(); err != nil || !result.Changed {
				t.Fatalf("re-stamp absorbed a field the carried record never had: %+v %v", result, err)
			}
			delete(api.service.Annotations, "example.com/maintenance")
			if result, err := diff(); err != nil || result.Changed != tc.changed {
				t.Fatalf("carried acceptance point: changed=%v, want %v: %+v %v", result.Changed, tc.changed, result, err)
			}
			if (api.renders > 0) != tc.stripped {
				t.Fatalf("default render runs only when no record can be carried: renders=%d", api.renders)
			}
		})
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

// Records written before UID binding stay readable, and adoption upgrades
// them in place, carrying their additions forward.
func TestAdoptionUpgradesRecordWithoutUID(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if err := controller.StepEvents(ctx, time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	legacy := serviceObject(t, &api.service)
	legacy.SetUID("")
	if err := RecordAcceptance(legacy, source.object); err != nil {
		t.Fatal(err)
	}
	api.service.Annotations[acceptedPathsAnnotation] = legacy.GetAnnotations()[acceptedPathsAnnotation]
	api.service.Annotations["example.com/maintenance"] = "true"
	diff := func() (program.Diff, error) {
		snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*api.service.DeepCopy()}}
		return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}.DiffAgainstSource(ref)
	}
	if result, err := diff(); err != nil || !result.Changed {
		t.Fatalf("record without UID not read: %+v %v", result, err)
	}
	controller.adoptionNeeded = true
	if err := controller.StepEvents(ctx, time.Unix(11, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	if !acceptanceRecordBound(serviceObject(t, &api.service), source.object) {
		t.Fatal("adoption did not bind the record to the object UID")
	}
	if result, err := diff(); err != nil || !result.Changed {
		t.Fatalf("upgrade absorbed an addition: %+v %v", result, err)
	}
}

