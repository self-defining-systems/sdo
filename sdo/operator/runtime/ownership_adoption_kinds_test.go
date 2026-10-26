package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type multiAdoptionSource struct{ objects []*unstructured.Unstructured }

func (s multiAdoptionSource) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	result := make([]*unstructured.Unstructured, 0, len(s.objects))
	for _, object := range s.objects {
		result = append(result, object.DeepCopy())
	}
	return result, nil
}

// genericAdoptionAPI serves discovery for core/v1 and policy/v1 plus GET and
// server-side apply for stored objects of any kind, keyed by request path.
type genericAdoptionAPI struct {
	mu       sync.Mutex
	objects  map[string]map[string]any
	requests []string
	applies  int
}

func (a *genericAdoptionAPI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, request.Method+" "+request.URL.Path)
	writer.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/api/v1":
		_ = json.NewEncoder(writer).Encode(&metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "persistentvolumeclaims", Kind: "PersistentVolumeClaim", Namespaced: true},
			{Name: "persistentvolumeclaims/status", Kind: "PersistentVolumeClaim", Namespaced: true},
			{Name: "secrets", Kind: "Secret", Namespaced: true},
		}})
		return
	case "/apis/policy/v1":
		_ = json.NewEncoder(writer).Encode(&metav1.APIResourceList{GroupVersion: "policy/v1", APIResources: []metav1.APIResource{{Name: "poddisruptionbudgets", Kind: "PodDisruptionBudget", Namespaced: true}}})
		return
	}
	stored, ok := a.objects[request.URL.Path]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(writer).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound})
		return
	}
	if request.Method == http.MethodGet {
		_ = json.NewEncoder(writer).Encode(stored)
		return
	}
	query := request.URL.Query()
	if request.Method != http.MethodPatch || query.Get("fieldManager") != "sdo-deployer" || query.Get("force") != "true" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	body, _ := io.ReadAll(request.Body)
	applied := map[string]any{}
	if json.Unmarshal(body, &applied) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	result := (&unstructured.Unstructured{Object: stored}).DeepCopy()
	result.Object["spec"] = applied["spec"]
	if query.Get("dryRun") == "" {
		a.applies++
		result.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{}}`)}}})
		a.objects[request.URL.Path] = result.Object
	}
	_ = json.NewEncoder(writer).Encode(result.Object)
}

func adoptionObject(apiVersion, kind, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": map[string]any{"name": name, "namespace": "app"}, "spec": spec}}
}

func seededLive(source *unstructured.Unstructured) map[string]any {
	live := source.DeepCopy()
	live.SetUID(types.UID("uid-" + source.GetName()))
	live.SetResourceVersion("1")
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl-client-side-apply", Operation: metav1.ManagedFieldsOperationUpdate, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{}}`)}}})
	return live.Object
}

func genericAdoptionController(t *testing.T, sources []*unstructured.Unstructured, live map[string]map[string]any) (*Controller, *genericAdoptionAPI) {
	t.Helper()
	api := &genericAdoptionAPI{objects: live}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", "accepted"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: multiAdoptionSource{objects: sources}, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}}
	}})
	return controller, api
}

func TestControllerAdoptsAnyServedKind(t *testing.T) {
	pvc := adoptionObject("v1", "PersistentVolumeClaim", "geo-pvc", map[string]any{"accessModes": []any{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}})
	pdb := adoptionObject("policy/v1", "PodDisruptionBudget", "opensearch-pdb", map[string]any{"maxUnavailable": float64(1), "selector": map[string]any{"matchLabels": map[string]any{"app": "opensearch"}}})
	secret := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "creds", "namespace": "app"}, "stringData": map[string]any{"password": "accepted"}}}
	live := map[string]map[string]any{
		"/api/v1/namespaces/app/persistentvolumeclaims/geo-pvc":              seededLive(pvc),
		"/apis/policy/v1/namespaces/app/poddisruptionbudgets/opensearch-pdb": seededLive(pdb),
		"/api/v1/namespaces/app/secrets/creds":                               seededLive(secret),
	}
	controller, api := genericAdoptionController(t, []*unstructured.Unstructured{pvc, pdb, secret}, live)
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.LastError != "" || status.AdoptedObjects != 2 || api.applies != 2 || len(status.FromManagers) != 1 || status.FromManagers[0] != "kubectl-client-side-apply" {
		t.Fatalf("PVC and PDB were not adopted: status=%+v applies=%d requests=%v", status, api.applies, api.requests)
	}
	for _, request := range api.requests {
		if strings.Contains(request, "/secrets/") {
			t.Fatalf("adoption touched Secret data: %v", api.requests)
		}
	}
}

func TestControllerAdoptionFailsClosedForUnservedKind(t *testing.T) {
	widget := adoptionObject("example.com/v1", "Widget", "w", map[string]any{"size": float64(1)})
	controller, api := genericAdoptionController(t, []*unstructured.Unstructured{widget}, map[string]map[string]any{})
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if !strings.Contains(status.LastError, "unsupported accepted manifest Widget/app/w") || status.AdoptedObjects != 0 || api.applies != 0 {
		t.Fatalf("unserved kind did not fail closed: status=%+v applies=%d", status, api.applies)
	}
}

func TestControllerAdoptionRejectsNamespacedObjectOutsideNamespace(t *testing.T) {
	pvc := adoptionObject("v1", "PersistentVolumeClaim", "data", map[string]any{"accessModes": []any{"ReadWriteOnce"}})
	pvc.SetNamespace("other")
	controller, api := genericAdoptionController(t, []*unstructured.Unstructured{pvc}, map[string]map[string]any{})
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if !strings.Contains(status.LastError, "outside namespace app") || api.applies != 0 {
		t.Fatalf("foreign-namespace object was not rejected: status=%+v applies=%d", status, api.applies)
	}
}

// serveCoreDiscovery answers core/v1 discovery for single-purpose API fakes.
func serveCoreDiscovery(writer http.ResponseWriter, request *http.Request) bool {
	if request.URL.Path != "/api/v1" {
		return false
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(&metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "services", Kind: "Service", Namespaced: true}}})
	return true
}
