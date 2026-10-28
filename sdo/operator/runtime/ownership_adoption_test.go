package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type adoptionSource struct {
	object *unstructured.Unstructured
	err    error
	calls  *int
}

func (s adoptionSource) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	if s.calls != nil {
		*s.calls++
	}
	if s.err != nil {
		return nil, s.err
	}
	return []*unstructured.Unstructured{s.object.DeepCopy()}, nil
}

func (s adoptionSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.object.DeepCopy(), nil
}

type adoptionServiceAPI struct {
	service     corev1.Service
	patches     int
	dryRuns     int
	afterDryRun func(*corev1.Service)
	conflicts   int
	lastApply   *corev1.Service
	records     int
	renders     int
	// owners models server-side apply for annotations: a manager's apply
	// removes the keys it applied before and omits now.
	owners map[string]string
}

func (a *adoptionServiceAPI) applyAnnotations(manager string, applied map[string]string) {
	if a.owners == nil {
		a.owners = map[string]string{}
	}
	for key, owner := range a.owners {
		if _, kept := applied[key]; owner == manager && !kept {
			delete(a.service.Annotations, key)
			delete(a.owners, key)
		}
	}
	for key, value := range applied {
		if a.service.Annotations == nil {
			a.service.Annotations = map[string]string{}
		}
		a.service.Annotations[key] = value
		a.owners[key] = manager
	}
}

func (a *adoptionServiceAPI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if serveTestDiscovery(writer, request) {
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if request.Method == http.MethodGet {
		_ = json.NewEncoder(writer).Encode(&a.service)
		return
	}
	manager := request.URL.Query().Get("fieldManager")
	if request.Method != http.MethodPatch || (manager != "sdo-deployer" && manager != acceptanceFieldManager) || request.URL.Query().Get("force") != "true" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	var applied corev1.Service
	if json.NewDecoder(request.Body).Decode(&applied) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	// A dry-run apply at another name creates nothing: it renders the body
	// with the defaults the server fills into a new Service.
	if !strings.HasSuffix(request.URL.Path, "/"+a.service.Name) {
		if request.URL.Query().Get("dryRun") == "" || !strings.HasSuffix(request.URL.Path, "/"+applied.Name) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		a.renders++
		if applied.Spec.Type == "" {
			applied.Spec.Type = corev1.ServiceTypeClusterIP
		}
		if applied.Spec.SessionAffinity == "" {
			applied.Spec.SessionAffinity = corev1.ServiceAffinityNone
		}
		_ = json.NewEncoder(writer).Encode(&applied)
		return
	}
	if manager == acceptanceFieldManager {
		if request.URL.Query().Get("dryRun") != "" || len(applied.Spec.Ports) != 0 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		a.records++
		a.applyAnnotations(manager, applied.Annotations)
		a.service.ResourceVersion = fmt.Sprint(a.patches + a.records + 1)
		_ = json.NewEncoder(writer).Encode(&a.service)
		return
	}
	if request.URL.Query().Get("dryRun") == "" {
		a.lastApply = applied.DeepCopy()
		if (applied.UID != "" && applied.UID != a.service.UID) || (applied.ResourceVersion != "" && applied.ResourceVersion != a.service.ResourceVersion) {
			a.conflicts++
			writer.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(writer).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonConflict, Code: http.StatusConflict, Message: "object changed since check"})
			return
		}
	}
	result := *a.service.DeepCopy()
	result.Spec.Ports[0].TargetPort = applied.Spec.Ports[0].TargetPort
	if request.URL.Query().Get("dryRun") == "" {
		a.patches++
		result.ResourceVersion = fmt.Sprint(a.patches + a.records + 1)
		result.ManagedFields = append(result.ManagedFields, metav1.ManagedFieldsEntry{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:port":{},"f:protocol":{},"f:targetPort":{}}}}}`)}})
		a.service = result
		a.applyAnnotations(manager, applied.Annotations)
	} else {
		a.dryRuns++
		if a.afterDryRun != nil {
			mutate := a.afterDryRun
			a.afterDryRun = nil
			mutate(&a.service)
		}
	}
	_ = json.NewEncoder(writer).Encode(&result)
}

func TestControllerAdoptsSeededServiceThenDispatchesLaterDrift(t *testing.T) {
	controller, api, _, closeServer := adoptionController(t, 8080)
	defer closeServer()
	start := time.Unix(10, 0).UTC()
	seeded := api.service.DeepCopy()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.AdoptedObjects != 1 || status.ConflictsTakenOver == 0 || len(status.FromManagers) != 1 || status.FromManagers[0] != "kubectl-client-side-apply" || len(exportState(t, controller).DriftEvents) != 0 {
		t.Fatalf("adoption status or drift wrong: %+v", exportState(t, controller))
	}
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{objectChange(t, seeded, &api.service, "v1", "Service")}}
	if err := controller.StepEvents(context.Background(), start.Add(500*time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	if len(exportState(t, controller).DriftEvents) != 0 {
		t.Fatalf("ownership-only update entered drift feed: %+v", exportState(t, controller).DriftEvents)
	}
	before := api.service.DeepCopy()
	api.service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	api.service.ManagedFields = append(api.service.ManagedFields, metav1.ManagedFieldsEntry{Manager: "kubectl-patch", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: before.ManagedFields[0].FieldsV1})
	change := objectChange(t, before, &api.service, "v1", "Service")
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{change}}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	events := exportState(t, controller).DriftEvents
	if len(events) != 1 || events[0].Classification != string(sdk.DriftUnexplained) || events[0].Fields[0].Path != "spec.ports[0].targetPort" || len(exportState(t, controller).PendingBatch.Findings) != 1 {
		t.Fatalf("later drift did not reach responder admission: %+v", exportState(t, controller))
	}
	if err := controller.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() || exportState(t, controller).IncidentRequest.Findings[0].DetectorId != "drift.unexplained" {
		t.Fatalf("drift did not open responder incident: %+v", exportState(t, controller))
	}

}

func TestControllerKeepsDetectingWhileAdoptionRetries(t *testing.T) {
	detector := controllerDetector("health", time.Second, sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
	controller, _, source, closeServer := adoptionController(t, 8080, detector)
	defer closeServer()
	calls := 0
	source.err = errors.New("accepted manifest source temporarily unavailable")
	source.calls = &calls
	controller.drift.Source = source
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.LastError == "" || !controllerTime(status.RetryAt).After(start) || calls != 1 {
		t.Fatalf("adoption failure status = %+v; calls=%d", status, calls)
	}
	payload, err := encodeControllerState(exportState(t, controller))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := decodeControllerState(payload)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.OwnershipAdoption.LastError != status.LastError || !controllerTime(persisted.OwnershipAdoption.RetryAt).Equal(controllerTime(status.RetryAt)) {
		t.Fatalf("adoption retry status was not durable: %+v", persisted.OwnershipAdoption)
	}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if detector.calls < 2 || calls != 1 {
		t.Fatalf("health stopped or adoption ignored backoff: detector=%d adoption=%d", detector.calls, calls)
	}
	controller.drift.Source = adoptionSource{object: source.object, calls: &calls}
	if err := controller.StepEvents(context.Background(), controllerTime(status.RetryAt), nil); err != nil {
		t.Fatal(err)
	}
	status = exportState(t, controller).OwnershipAdoption
	if status.LastError != "" || status.AdoptedObjects != 1 || calls != 2 {
		t.Fatalf("adoption did not recover: %+v; calls=%d", status, calls)
	}
}

func TestControllerRecordsSkippedAdoptionWithoutDeployedPlan(t *testing.T) {
	controller, _, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	source.err = deployment.ErrManifestSelectionUnavailable
	controller.drift.Source = source
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.SkippedReason == "" || status.LastError != "" || status.AdoptedObjects != 0 || controller.adoptionNeeded {
		t.Fatalf("unknown deployed set was adopted or hidden: %+v", status)
	}
}

func TestControllerLeavesPreexistingDifferenceForInvestigation(t *testing.T) {
	controller, api, _, closeServer := adoptionController(t, 9999)
	defer closeServer()
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	events := exportState(t, controller).DriftEvents
	if api.patches != 0 || api.service.Spec.Ports[0].TargetPort.IntVal != 9999 || len(events) != 1 || events[0].Classification != string(sdk.DriftUnexplained) || events[0].Fields[0].Path != "spec.ports[0].targetPort" || len(exportState(t, controller).PendingBatch.Findings) != 1 {
		t.Fatalf("preexisting fault was overwritten or lost: %+v, %+v", api, exportState(t, controller))
	}
}

func TestControllerDoesNotReinvestigateUnchangedAdoptedDifference(t *testing.T) {
	controller, _, _, closeServer := adoptionController(t, 9999)
	defer closeServer()
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	controller.adoptionNeeded = true
	if err := controller.EvaluateAll(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(exportState(t, controller).PendingBatch.Findings) != 1 {
		t.Fatal("pending exact drift was duplicated")
	}

}

func TestControllerAdoptionIsIdempotentAndNotDrift(t *testing.T) {
	controller, api, _, closeServer := adoptionController(t, 8080)
	defer closeServer()
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.EvaluateAll(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if api.patches != 1 || api.dryRuns != 2 || exportState(t, controller).OwnershipAdoption.AdoptedObjects != 1 || len(exportState(t, controller).DriftEvents) != 0 {
		t.Fatalf("adoption repeated or reported as drift: %+v, %+v", api, exportState(t, controller))
	}
}

func TestControllerReadoptsNewDeclaredFieldAtAcceptedCommit(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	start := time.Unix(10, 0).UTC()
	if err := controller.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	api.service.Spec.Ports[0].Name = "web"
	source.object.Object["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["name"] = "web"
	controller.config.SourceCommit = "next-accepted"
	if err := controller.EvaluateAll(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if api.patches != 2 || status.SourceCommit != "next-accepted" || status.AdoptedObjects != 1 || len(exportState(t, controller).DriftEvents) != 0 {
		t.Fatalf("accepted source was not re-adopted: %+v, %+v", api, exportState(t, controller))
	}
}

type fixedObjectChanges struct{ changes []ObjectChange }

func (f *fixedObjectChanges) TakeObjectChanges() []ObjectChange {
	changes := f.changes
	f.changes = nil
	return changes
}

func adoptionController(t *testing.T, target int32, detectors ...sdk.Detector) (*Controller, *adoptionServiceAPI, adoptionSource, func()) {
	t.Helper()
	service := *driftService(target)
	service.UID, service.ResourceVersion = "original", "1"
	service.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	service.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl-client-side-apply", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}}}
	api := &adoptionServiceAPI{service: service}
	server := httptest.NewServer(api)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	source := adoptionSource{object: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80), "protocol": "TCP", "targetPort": float64(8080)}}}}}}
	provider := adoptionSnapshot{api: api}
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", "accepted"
	controller, err := NewController(config, detectors, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: source, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: api.service.Spec.Ports[0].TargetPort.IntVal != 8080}}
	}})
	return controller, api, source, server.Close
}

type adoptionSnapshot struct{ api *adoptionServiceAPI }

func (s adoptionSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{s.api.service}}, nil
}

func TestControllerAdoptionDoesNotOverwriteConcurrentMutation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		recreate, drift bool
	}{
		{name: "declared drift", drift: true},
		{name: "delete and recreate", recreate: true, drift: true},
		{name: "unrelated update"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller, api, source, closeServer := adoptionController(t, 8080)
			defer closeServer()
			// This view uses the controller's earlier snapshot. A conflict must
			// classify the freshly read object even if the informer is stale.
			controller.drift.View = func(snapshot sdk.DetectionContext) program.ReadView {
				return LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}
			}
			api.afterDryRun = func(service *corev1.Service) {
				service.ResourceVersion = "2"
				if tc.recreate {
					service.UID = "replacement"
					service.ResourceVersion = "1"
				}
				if tc.drift {
					service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
				}
			}
			if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
				t.Fatal(err)
			}
			state := exportState(t, controller)
			if api.patches != 0 || api.conflicts != 1 || api.lastApply == nil || api.lastApply.UID != "original" || api.lastApply.ResourceVersion != "1" {
				t.Fatalf("adoption lacked checked preconditions or retried a conflict: patches=%d conflicts=%d apply=%+v", api.patches, api.conflicts, api.lastApply)
			}
			if state.OwnershipAdoption.AdoptedObjects != 0 || state.OwnershipAdoption.LastError == "" {
				t.Fatalf("conflict reported as adoption: %+v", state.OwnershipAdoption)
			}
			if tc.drift {
				if api.service.Spec.Ports[0].TargetPort.IntVal != 9999 || len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != string(sdk.DriftUnexplained) || len(state.PendingBatch.Findings) != 1 {
					t.Fatalf("concurrent drift overwritten or not classified: service=%+v state=%+v", api.service, state)
				}
			} else if len(state.DriftEvents) != 0 {
				t.Fatalf("unrelated update classified as drift: %+v", state.DriftEvents)
			}
		})
	}
}

func TestAcceptanceRecordSurvivesDeployAndReacceptance(t *testing.T) {
	controller, api, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	diff := func(commit string) (program.Diff, error) {
		snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*api.service.DeepCopy()}}
		return LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: commit}.DiffAgainstSource(ref)
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
	if _, err := (LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}).DiffAgainstSource(ref); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("missing acceptance record compared as known: %v", err)
	}
	api.service.Spec.Ports[0].TargetPort.IntVal = 9090
	snapshot.ServiceList = []corev1.Service{api.service}
	if result, err := (LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: "accepted"}).DiffAgainstSource(ref); err != nil || !result.Changed {
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
		return LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source.object}, SourceCommit: "accepted", Client: client}
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
