package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
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
	if request.Method == http.MethodPatch && query.Get("fieldManager") == acceptanceFieldManager && query.Get("dryRun") == "" {
		a.recordAcceptance(writer, request, stored)
		return
	}
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

// recordAcceptance applies a metadata-only acceptance record; anything else
// under that field manager is rejected.
func (a *genericAdoptionAPI) recordAcceptance(writer http.ResponseWriter, request *http.Request, stored map[string]any) {
	body, _ := io.ReadAll(request.Body)
	applied := &unstructured.Unstructured{}
	if json.Unmarshal(body, &applied.Object) != nil || applied.Object["spec"] != nil || len(applied.GetAnnotations()) != 1 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	result := (&unstructured.Unstructured{Object: stored}).DeepCopy()
	annotations := result.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[acceptedPathsAnnotation] = applied.GetAnnotations()[acceptedPathsAnnotation]
	result.SetAnnotations(annotations)
	a.objects[request.URL.Path] = result.Object
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
