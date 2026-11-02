//go:build integration

package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func TestControllerAdoptsAvailableObjectsWithExplicitRenderGap(t *testing.T) {
	controller, api, _, closeServer := adoptionController(t, 8080)
	defer closeServer()
	source, _, commit, _, _ := acceptedSourceDeployment(t, true)
	source.Images = nil // A cold process has no healthy workload image evidence yet.
	controller.config.SourceCommit = commit
	controller.drift.Source = source
	api.service.Spec.Selector = map[string]string{"app": "api"}
	controller.drift.View = func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceCommit: commit, Client: controller.drift.Client}
	}
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.AdoptedObjects != 1 || status.LastError == "" || !controller.adoptionNeeded || status.RetryAt == nil {
		t.Fatalf("partial adoption hid coverage gap or lost service: %+v", status)
	}
	before := api.service.DeepCopy()
	api.service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{objectChange(t, before, &api.service, "v1", "Service")}}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	events := exportState(t, controller).DriftEvents
	if len(events) != 1 || events[0].Classification != string(sdk.DriftUnexplained) {
		t.Fatalf("partial render gap silenced service drift: %+v", events)
	}
	if err := controller.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() || exportState(t, controller).IncidentRequest.Findings[0].DetectorId != "drift.unexplained" {
		t.Fatalf("service drift never opened an incident: %+v", exportState(t, controller))
	}
}

func TestAcceptanceRecordSurvivesDeployAndReacceptance(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	diff := func(commit string) (program.Diff, error) {
		snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*api.service.DeepCopy()}}
		return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: commit}.DiffAgainstSource(ref)
	}
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(ctx, start, nil); err != nil {
		t.Fatal(err)
	}
	if result, err := diff("accepted"); err != nil || result.Changed {
		t.Fatalf("adopted Service not accepted: %+v %v", result, err)
	}
	api.service.Annotations["example.com/maintenance"] = "true"
	if result, err := diff("accepted"); err != nil || !result.Changed {
		t.Fatalf("annotation added after acceptance not reported: %+v %v", result, err)
	}
	if _, err := applyAcceptedSource(ctx, controller.drift.Client, ref, source.object, false); err != nil {
		t.Fatal(err)
	}
	if result, err := diff("accepted"); err != nil || !result.Changed {
		t.Fatalf("deploy erased the acceptance point: %+v %v", result, err)
	}

	// Accepted source changes shape: the old record no longer applies, so
	// the comparison is unknown until SDO accepts the object again.
	api.service.Spec.Ports[0].Name = "web"
	source.object.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["name"] = "web"
	if _, err := diff("next-accepted"); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("stale acceptance record compared: %v", err)
	}
	controller.config.SourceCommit = "next-accepted"
	controller.adoptionNeeded = true
	if err := controller.EvaluateAll(ctx, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if result, err := diff("next-accepted"); err != nil || !result.Changed {
		t.Fatalf("re-acceptance absorbed the earlier addition: %+v %v", result, err)
	}
	delete(api.service.Annotations, "example.com/maintenance")
	if result, err := diff("next-accepted"); err != nil || result.Changed {
		t.Fatalf("removed addition still reported: %+v %v", result, err)
	}
}

// An object recreated or stripped of its record has no acceptance point: the
// comparison fails closed instead of reporting it unchanged.
func TestSourceDiffFailsClosedWithoutAcceptanceRecord(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	delete(api.service.Annotations, acceptedPathsAnnotation)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{api.service}}
	if _, err := (LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}).DiffAgainstSource(ref); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("missing acceptance record compared as known: %v", err)
	}
	api.service.Spec.Ports[0].TargetPort.IntVal = 9090
	snapshot.ServiceList = []corev1.Service{api.service}
	if result, err := (LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}).DiffAgainstSource(ref); err != nil || !result.Changed {
		t.Fatalf("declared difference hidden by a missing record: %+v %v", result, err)
	}
}

// Controllers write some fields after SDO applies an object (a rollout's
// observed spec, a scheduled Pod's node). The record waits until the object
// has settled, so those writes never count as additions.
func TestAdoptionRecordsAcceptanceOnlyOnceSettled(t *testing.T) {
	ctx := context.Background()
	one := int32(1)
	labelsOf := map[string]string{"app": "search"}
	live := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app", Generation: 2, UID: "search-uid", ResourceVersion: "1"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labelsOf}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labelsOf}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "search", Image: "search:1"}}}}},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
	fakeClient := fake.NewSimpleClientset(live)
	client := bridgeFake(t, fakeClient)
	source := adoptionSource{object: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "search", "namespace": "app"}, "spec": map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "search", "image": "search:1"}}}}}}}}
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", "accepted"
	controller, err := NewController(config, nil, fastloopObjects{client: fakeClient}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: source, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: source.object}, SourceCommit: "accepted", Client: client}
	}})
	recorded := func() bool {
		t.Helper()
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
		resource, err := resolveAcceptedResource(client, ref)
		if err != nil {
			t.Fatal(err)
		}
		object, err := resource.get(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		return acceptanceRecordCurrent(object, source.object)
	}
	if err := controller.adoptOwnership(ctx, time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if recorded() || !controller.adoptionNeeded {
		t.Fatal("acceptance recorded before the rollout controller observed the spec")
	}
	settled, err := client.AppsV1().Deployments("app").Get(ctx, "search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	settled.Status.ObservedGeneration = settled.Generation
	if _, err := client.AppsV1().Deployments("app").UpdateStatus(ctx, settled, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := controller.adoptOwnership(ctx, time.Unix(17, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if !recorded() || controller.adoptionNeeded {
		t.Fatal("settled object was not recorded")
	}
}
