package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestMissingAcceptanceRearmsAdoption(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "demo"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"replicas": int64(1)}}}
	c := &Controller{}
	c.ConfigureDrift(DriftControllerConfig{View: func(v sdk.DetectionContext) program.ReadView {
		return stageDriftView{ReadView: program.ReadFrom(v), err: &AcceptanceUnknownError{Ref: ref}}
	}})
	c.adoptionNeeded = false
	old := object.DeepCopy()
	old.SetAnnotations(map[string]string{acceptedPathsAnnotation: `{"source":"earlier-source","uid":"old-uid"}`})
	_ = unstructured.SetNestedField(old.Object, int64(2), "spec", "replicas")
	c.processDrift(context.Background(), time.Unix(1, 0), sdktest.Snapshot{NamespaceName: "app"}, []ObjectChange{{Old: old, New: object.DeepCopy(), Adoption: true}})
	if !c.adoptionNeeded {
		t.Fatal("lost acceptance point left adoption permanently disabled")
	}
}

type retryAcceptanceSource struct{ object *unstructured.Unstructured }

func (s retryAcceptanceSource) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	return []*unstructured.Unstructured{s.object.DeepCopy()}, nil
}

func TestAdoptionWaitsForRepairBeforeRecordingMissingProvenance(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config", "namespace": "demo"}, "data": map[string]any{"mode": "accepted"}}}
	live := source.DeepCopy()
	live.SetUID("same-instance")
	live.SetResourceVersion("1")
	_ = unstructured.SetNestedField(live.Object, "faulted", "data", "mode")
	writes := 0
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://synthetic.invalid", Transport: budgetRoundTripper(func(r *http.Request) (*http.Response, error) {
		var value any
		switch {
		case r.URL.Path == "/api/v1":
			value = metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true}}}
		case r.Method == "GET":
			value = live.Object
		case r.URL.Query().Get("dryRun") == "All":
			value = source.Object
		default:
			writes++
			var applied unstructured.Unstructured
			if err := json.NewDecoder(r.Body).Decode(&applied.Object); err != nil {
				return nil, err
			}
			if annotations := applied.GetAnnotations(); len(annotations) > 0 {
				live.SetAnnotations(annotations)
			}
			value = live.Object
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "demo"}
	c.ConfigureDrift(DriftControllerConfig{Source: retryAcceptanceSource{source}, Client: client, View: func(v sdk.DetectionContext) program.ReadView {
		return stageDriftView{ReadView: program.ReadFrom(v), diff: program.Diff{Changed: true}}
	}})
	if err := c.adoptOwnership(context.Background(), time.Unix(1, 0), snapshot); err != nil {
		t.Fatal(err)
	}
	if !c.adoptionNeeded || c.ownershipAdoption.RetryAt.IsZero() {
		t.Fatal("adoption stopped before the faulted object could acquire provenance")
	}
	if writes != 0 {
		t.Fatal("faulted live fields were accepted")
	}
	_ = unstructured.SetNestedField(live.Object, "accepted", "data", "mode")
	if err := c.adoptOwnership(context.Background(), c.ownershipAdoption.RetryAt, snapshot); err != nil {
		t.Fatal(err)
	}
	if c.adoptionNeeded || !acceptanceRecordBound(live, source) {
		t.Fatal("repaired object never acquired its current UID-bound provenance")
	}
}
