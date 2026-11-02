//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"sdo.dev/operator/sdk/inputs"
	"testing"
	"time"

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
	server := newHandlerTransport(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveTestDiscovery(writer, request) {
			return
		}
		if request.Method != http.MethodPatch || request.URL.Query().Get("dryRun") != metav1.DryRunAll {
			t.Errorf("unexpected source normalization request: %s %s", request.Method, request.URL)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8080)}}}})
	}))

	apiClient, err := kubernetes.NewForConfig(&rest.Config{Host: "http://in-memory", Transport: server})
	if err != nil {
		t.Fatal(err)
	}
	source := planFakeSource{source: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api", "namespace": "app"}, "spec": map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}}}}}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Client: apiClient, Source: source, SourceCommit: "accepted"}
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

