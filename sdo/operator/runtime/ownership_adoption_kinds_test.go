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

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
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

// genericAdoptionAPI serves test discovery plus GET and
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
	if serveTestDiscovery(writer, request) {
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

func (s multiAdoptionSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	for _, object := range s.objects {
		if object.GetKind() == ref.Kind && object.GetName() == ref.Name {
			return object.DeepCopy(), nil
		}
	}
	return nil, nil
}

func (s multiAdoptionSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func driftPDB(minAvailable int32, manager string, operation metav1.ManagedFieldsOperationType) *policyv1.PodDisruptionBudget {
	value := intstr.FromInt32(minAvailable)
	return &policyv1.PodDisruptionBudget{
		TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		ObjectMeta: metav1.ObjectMeta{Name: "opensearch-pdb", Namespace: "app", UID: "pdb", ResourceVersion: "1", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: manager, Operation: operation, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:minAvailable":{}}}`)}}}},
		Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &value},
	}
}

var acceptedPDB = adoptionObject("policy/v1", "PodDisruptionBudget", "opensearch-pdb", map[string]any{"minAvailable": float64(1)})

// A declared field of an adopted kind beyond the old typed set is compared
// with accepted source, so its change is unexplained drift.
func TestDriftClassifierComparesAdoptedPDBWithSource(t *testing.T) {
	old := driftPDB(1, "sdo-deployer", metav1.ManagedFieldsOperationApply)
	next := driftPDB(0, "kubectl-patch", metav1.ManagedFieldsOperationUpdate)
	next.ManagedFields = append(old.ManagedFields, next.ManagedFields...)
	client := bridgeFake(t, fake.NewSimpleClientset(next.DeepCopy()))
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", PDBList: []policyv1.PodDisruptionBudget{*next}}, Client: client, Source: multiAdoptionSource{objects: []*unstructured.Unstructured{acceptedPDB}}, SourceCommit: "accepted"}
	diff, err := view.DiffAgainstSource(sdk.ObjectRef{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "app", Name: "opensearch-pdb"})
	if err != nil || !diff.Changed {
		t.Fatalf("PDB source diff = %+v, %v", diff, err)
	}
	event, err := (DriftClassifier{}).Classify(context.Background(), objectChange(t, old, next, "policy/v1", "PodDisruptionBudget"), view, time.Unix(10, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained || event.Fields[0].Path != "spec.minAvailable" {
		t.Fatalf("declared PDB change was not unexplained drift: %+v, %v", event, err)
	}
}

// A PDB that already differs from source at adoption is recorded once for
// investigation; adoption does not fail on every iteration.
func TestAdoptionRecordsPreexistingPDBDifferenceOnce(t *testing.T) {
	live := driftPDB(0, "kubectl-client-side-apply", metav1.ManagedFieldsOperationUpdate)
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(live)
	if err != nil {
		t.Fatal(err)
	}
	controller, api := genericAdoptionController(t, []*unstructured.Unstructured{acceptedPDB}, map[string]map[string]any{"/apis/policy/v1/namespaces/app/poddisruptionbudgets/opensearch-pdb": content})
	client := controller.drift.Client
	controller.drift.View = func(sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", PDBList: []policyv1.PodDisruptionBudget{*live}}, Client: client, Source: multiAdoptionSource{objects: []*unstructured.Unstructured{acceptedPDB}}, SourceCommit: "accepted"}
	}
	start := time.Unix(10, 0).UTC()
	for step := 0; step < 3; step++ {
		if err := controller.StepEvents(context.Background(), start.Add(time.Duration(step)*time.Minute), nil); err != nil {
			t.Fatal(err)
		}
	}
	state := exportState(t, controller)
	if state.OwnershipAdoption.LastError != "" || state.OwnershipAdoption.Failures != 0 || api.applies != 0 {
		t.Fatalf("pre-existing PDB difference broke adoption: %+v applies=%d", state.OwnershipAdoption, api.applies)
	}
	if len(state.DriftEvents) != 1 || state.DriftEvents[0].Classification != string(sdk.DriftUnexplained) {
		t.Fatalf("pre-existing PDB difference was not recorded once: %+v", state.DriftEvents)
	}
}

// Adoption claims no field ownership of shared cluster objects outside the
// app's own RBAC grants, and declaring one does not block the namespace.
func TestAdoptionLeavesSharedClusterObjectsUnowned(t *testing.T) {
	storageClass := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": map[string]any{"name": "fast"}, "provisioner": "example.com/disk"}}
	pvc := adoptionObject("v1", "PersistentVolumeClaim", "data", map[string]any{"accessModes": []any{"ReadWriteOnce"}})
	live := map[string]map[string]any{
		"/apis/storage.k8s.io/v1/storageclasses/fast":        seededLive(storageClass),
		"/api/v1/namespaces/app/persistentvolumeclaims/data": seededLive(pvc),
	}
	controller, api := genericAdoptionController(t, []*unstructured.Unstructured{storageClass, pvc}, live)
	if err := controller.StepEvents(context.Background(), time.Unix(10, 0).UTC(), nil); err != nil {
		t.Fatal(err)
	}
	status := exportState(t, controller).OwnershipAdoption
	if status.LastError != "" || status.AdoptedObjects != 1 || api.applies != 1 {
		t.Fatalf("adoption status=%+v applies=%d", status, api.applies)
	}
	for _, request := range api.requests {
		if strings.Contains(request, "/storageclasses/") {
			t.Fatalf("adoption touched a shared cluster object: %v", api.requests)
		}
	}
}
