package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type driftReadView struct {
	*sdktest.ReplayReadView
	diff program.Diff
}

func (v driftReadView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) { return v.diff, nil }

func TestIncidentRequestCarriesOnlyFindingDriftAndPreservesStatusHistory(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	old := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "reservation"},
		ObservedAt: start, Classification: sdk.DriftUnexplained, InvestigationVerdict: "bug"}
	current := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"},
		ObservedAt: start.Add(time.Minute), Classification: sdk.DriftUnexplained}
	controller.driftEvents = []sdk.DriftEvent{old, current}
	finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive,
		PrimaryResource: current.Ref, Metadata: map[string]any{"drift_event": current}}
	request := controller.incidentRequest(start.Add(2*time.Minute), []sdk.Finding{finding})
	if len(request.DriftEvents) != 1 || request.DriftEvents[0].Ref != current.Ref {
		t.Fatalf("request carried unrelated historical drift: %+v", request.DriftEvents)
	}
	if events := exportState(t, controller).DriftEvents; len(events) != 2 || objectRefState(events[0].Ref) != old.Ref {
		t.Fatalf("status lost drift history: %+v", events)
	}
}

func TestDriftClassifierIgnoresHPAOwnedReplicas(t *testing.T) {
	before := int32(2)
	after := int32(3)
	old := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", ManagedFields: []metav1.ManagedFieldsEntry{
		{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:template":{}}}`)}},
		{Manager: "horizontal-pod-autoscaler", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}},
	}}, Spec: appsv1.DeploymentSpec{Replicas: &before}}
	next := old.DeepCopy()
	next.Spec.Replicas = &after
	event, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "apps/v1", "Deployment"), driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, diff: program.Diff{Changed: true}}, time.Unix(10, 0))
	if err != nil || event != nil {
		t.Fatalf("HPA replicas entered drift feed: %+v, %v", event, err)
	}
}

func TestDriftClassifierRecordsUnexplainedSDOChange(t *testing.T) {
	old := driftService(8080)
	next := driftService(8081)
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}, diff: program.Diff{Changed: true}}
	event, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "v1", "Service"), view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained {
		t.Fatalf("unexplained change lost: event=%+v err=%v", event, err)
	}
}

func TestDriftClassifierRecordsAddedDeclaredObject(t *testing.T) {
	added := driftService(9999)
	view := driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*added}}}, diff: program.Diff{Changed: true}}
	event, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{New: driftObject(added, sdk.WatchKind{APIVersion: "v1", Kind: "Service"})}, view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained {
		t.Fatalf("added SDO object was not classified: %+v, %v", event, err)
	}
}

func TestControllerOpensUnexplainedIncidentAfterBatchWindow(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	old := driftService(8080)
	next := driftService(9999)
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(old, next)
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		if request.Method != http.MethodPatch || request.URL.Query().Get("dryRun") != metav1.DryRunAll {
			t.Errorf("unexpected source normalization request: %s %s", request.Method, request.URL)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)}}}})
	}))
	defer server.Close()
	apiClient, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	source := planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}}}}}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Client: apiClient, Source: source, SourceCommit: "accepted"}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if controller.IncidentOpen() {
		t.Fatal("source drift bypassed its batch window")
	}
	if err := controller.StepEvents(context.Background(), start.Add(21*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, controller)
	if !state.IncidentOpen || len(state.IncidentRequest.Findings) != 1 || state.IncidentRequest.Findings[0].Severity != string(sdk.SeverityLatent) || len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != string(sdk.DriftUnexplained) || state.IncidentView == nil || len(state.IncidentView.DriftEvents) != 1 {
		t.Fatalf("latent incident not opened in one reconcile: %+v", state)
	}
}

func TestResponderStatusIncludesDriftClassification(t *testing.T) {
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, ObservedAt: time.Unix(10, 0).UTC(), Classification: sdk.DriftUnexplained}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Namespace: "app", DriftEvents: []sdk.DriftEvent{event}}, nil,
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, PlanExecutor{}, 1)
	status := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(status.DriftEvents) != 1 || status.DriftEvents[0].Classification != sdk.DriftUnexplained {
		t.Fatalf("status lost drift classification: %+v", status)
	}
}

func TestControllerPersistsUnexplainedChangeWithoutIncident(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	old := driftService(8080)
	next := driftService(8081)
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(old, next)
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*next}}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, controller)
	if state.IncidentOpen || len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != string(sdk.DriftUnexplained) {
		t.Fatalf("unexplained event was not retained: %+v", state)
	}
}

func driftService(target int32) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}}}}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(target)}}}}
}

func objectChange(t *testing.T, old, next any, version, kind string) ObjectChange {
	t.Helper()
	return ObjectChange{Old: driftObject(old, sdk.WatchKind{APIVersion: version, Kind: kind}), New: driftObject(next, sdk.WatchKind{APIVersion: version, Kind: kind})}
}
