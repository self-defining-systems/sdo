package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	service corev1.Service
	patches int
	dryRuns int
}

func (a *adoptionServiceAPI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if request.Method == http.MethodGet {
		_ = json.NewEncoder(writer).Encode(&a.service)
		return
	}
	if request.Method != http.MethodPatch || request.URL.Query().Get("fieldManager") != "sdo-deployer" || request.URL.Query().Get("force") != "true" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	var applied corev1.Service
	if json.NewDecoder(request.Body).Decode(&applied) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	result := *a.service.DeepCopy()
	result.Spec.Ports[0].TargetPort = applied.Spec.Ports[0].TargetPort
	if request.URL.Query().Get("dryRun") == "" {
		a.patches++
		result.ManagedFields = append(result.ManagedFields, metav1.ManagedFieldsEntry{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:port":{},"f:protocol":{},"f:targetPort":{}}}}}`)}})
		a.service = result
	} else {
		a.dryRuns++
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
	status := controller.ExportState().OwnershipAdoption
	if status.AdoptedObjects != 1 || status.ConflictsTakenOver == 0 || len(status.FromManagers) != 1 || status.FromManagers[0] != "kubectl-client-side-apply" || len(controller.ExportState().DriftEvents) != 0 {
		t.Fatalf("adoption status or drift wrong: %+v", controller.ExportState())
	}
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{objectChange(t, seeded, &api.service, "v1", "Service")}}
	if err := controller.StepEvents(context.Background(), start.Add(500*time.Millisecond), nil); err != nil {
		t.Fatal(err)
	}
	if len(controller.ExportState().DriftEvents) != 0 {
		t.Fatalf("ownership-only update entered drift feed: %+v", controller.ExportState().DriftEvents)
	}
	before := api.service.DeepCopy()
	api.service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	api.service.ManagedFields = append(api.service.ManagedFields, metav1.ManagedFieldsEntry{Manager: "kubectl-patch", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: before.ManagedFields[0].FieldsV1})
	change := objectChange(t, before, &api.service, "v1", "Service")
	controller.drift.Events = &fixedObjectChanges{changes: []ObjectChange{change}}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	events := controller.ExportState().DriftEvents
	if len(events) != 1 || events[0].Classification != sdk.DriftUnexplained || events[0].Fields[0].Path != "spec.ports[0].targetPort" || controller.driftInvestigation.Pending() != 1 {
		t.Fatalf("later drift did not reach investigator: %+v", controller.ExportState())
	}
	investigator := &adoptionInvestigator{result: &contracts.DriftInvestigationResult{Events: []*contracts.DriftEventInvestigation{{
		EventIndex: 0, Verdict: "bug", Reason: "target port is unserved", Evidence: []*contracts.DriftEvidence{
			{Kind: "source_contradiction", Observation: "accepted source targets 8080"},
			{Kind: "violated_relation", Observation: "no selected pod serves 9999"},
		},
	}}}}
	controller.driftInvestigation.config.Worker = investigator
	if err := controller.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if investigator.calls != 1 || !controller.IncidentOpen() || controller.ExportState().DriftEvents[0].InvestigationVerdict != "bug" {
		t.Fatalf("drift investigator failed to open latent incident: calls=%d state=%+v", investigator.calls, controller.ExportState())
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
	status := controller.ExportState().OwnershipAdoption
	if status.LastError == "" || !status.RetryAt.After(start) || calls != 1 {
		t.Fatalf("adoption failure status = %+v; calls=%d", status, calls)
	}
	wire, err := json.Marshal(controller.ExportState())
	if err != nil {
		t.Fatal(err)
	}
	var persisted RuntimeState
	if err := json.Unmarshal(wire, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.OwnershipAdoption.LastError != status.LastError || !persisted.OwnershipAdoption.RetryAt.Equal(status.RetryAt) {
		t.Fatalf("adoption retry status was not durable: %+v", persisted.OwnershipAdoption)
	}
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if detector.calls < 2 || calls != 1 {
		t.Fatalf("health stopped or adoption ignored backoff: detector=%d adoption=%d", detector.calls, calls)
	}
	controller.drift.Source = adoptionSource{object: source.object, calls: &calls}
	if err := controller.StepEvents(context.Background(), status.RetryAt, nil); err != nil {
		t.Fatal(err)
	}
	status = controller.ExportState().OwnershipAdoption
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
	status := controller.ExportState().OwnershipAdoption
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
	events := controller.ExportState().DriftEvents
	if api.patches != 0 || api.service.Spec.Ports[0].TargetPort.IntVal != 9999 || len(events) != 1 || events[0].Classification != sdk.DriftUnexplained || events[0].Fields[0].Path != "spec.ports[0].targetPort" || controller.driftInvestigation.Pending() != 1 {
		t.Fatalf("preexisting fault was overwritten or lost: %+v, %+v", api, controller.ExportState())
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
	if api.patches != 1 || api.dryRuns != 2 || controller.ExportState().OwnershipAdoption.AdoptedObjects != 1 || len(controller.ExportState().DriftEvents) != 0 {
		t.Fatalf("adoption repeated or reported as drift: %+v, %+v", api, controller.ExportState())
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
	status := controller.ExportState().OwnershipAdoption
	if api.patches != 2 || status.SourceCommit != "next-accepted" || status.AdoptedObjects != 1 || len(controller.ExportState().DriftEvents) != 0 {
		t.Fatalf("accepted source was not re-adopted: %+v, %+v", api, controller.ExportState())
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
	}, Investigation: DriftInvestigationConfig{Worker: &adoptionInvestigator{}}})
	return controller, api, source, server.Close
}

type adoptionSnapshot struct{ api *adoptionServiceAPI }

func (s adoptionSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{s.api.service}}, nil
}

type adoptionInvestigator struct {
	calls  int
	result *contracts.DriftInvestigationResult
}

func (a *adoptionInvestigator) Investigate(context.Context, *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error) {
	a.calls++
	return a.result, nil
}
