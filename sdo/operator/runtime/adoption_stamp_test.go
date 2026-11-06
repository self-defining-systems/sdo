package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
)

// stampedDeploymentServer serves one deployer-stamped Deployment and records
// every apply body.
type stampedDeploymentServer struct {
	mu      sync.Mutex
	applied []map[string]any
}

func (s *stampedDeploymentServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPatch {
		body, _ := io.ReadAll(r.Body)
		var sent map[string]any
		_ = json.Unmarshal(body, &sent)
		s.applied = append(s.applied, sent)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "app", "uid": "u1", "resourceVersion": "7",
			"annotations": map[string]any{"sdo.dev/source-tree": "tree1"}},
		"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{
			"annotations": map[string]any{"sdo.dev/source-tree": "tree1"}}}},
	})
}

// Adopting a workload applies the unstamped accepted source as the deployer.
// A server-side apply that omits a field its manager owns deletes it, so the
// apply must carry the install stamp: otherwise every adopted Deployment rolls
// once, with no change to its images or configuration.
func TestAdoptionKeepsTheInstallStampSoTheWorkloadDoesNotRoll(t *testing.T) {
	server := &stampedDeploymentServer{}
	client, err := rest.UnversionedRESTClientFor(&rest.Config{
		Host:          "http://api.invalid",
		Transport:     handlerTransport{server},
		ContentConfig: rest.ContentConfig{NegotiatedSerializer: scheme.Codecs.WithoutConversion()},
	})
	if err != nil {
		t.Fatal(err)
	}
	resource := acceptedResource{client: client, path: "/apis/apps/v1/namespaces/app/deployments/web", namespaced: true}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "app"},
		"spec":     map[string]any{"replicas": int64(1)},
	}}
	if _, err := applyAdoption(context.Background(), resource, ref, source, source); err != nil {
		t.Fatal(err)
	}
	if len(server.applied) != 1 {
		t.Fatalf("want one apply, got %d", len(server.applied))
	}
	sent := &unstructured.Unstructured{Object: server.applied[0]}
	for _, path := range [][]string{{"metadata", "annotations"}, {"spec", "template", "metadata", "annotations"}} {
		got, _, _ := unstructured.NestedString(sent.Object, append(path, "sdo.dev/source-tree")...)
		if got != "tree1" {
			t.Errorf("apply dropped the stamp at %v: %v", path, server.applied[0])
		}
	}
}

// The accepted source can omit a field the deployer applied (a value the
// deployer's render set that the source render does not). A server-side apply
// that omits a field its manager owns deletes it, and a deleted non-default
// value changes the spec: generation moves with no change to the accepted
// state. Adoption carries every field the deployer owns that the source omits.
func TestAdoptionKeepsDeployerOwnedFieldsTheSourceOmits(t *testing.T) {
	live := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "app", "uid": "u1", "resourceVersion": "7",
			"managedFields": []any{map[string]any{
				"manager": "sdo-deployer", "operation": "Apply", "apiVersion": "apps/v1",
				"fieldsType": "FieldsV1",
				"fieldsV1": map[string]any{
					"f:spec": map[string]any{
						"f:replicas":        map[string]any{},
						"f:minReadySeconds": map[string]any{},
						"f:strategy":        map[string]any{"f:type": map[string]any{}},
						"f:template":        map[string]any{"f:metadata": map[string]any{"f:annotations": map[string]any{"f:example.com/x": map[string]any{}}}},
					},
				},
			}}},
		"spec": map[string]any{
			"replicas": int64(3), "minReadySeconds": int64(9), "strategy": map[string]any{"type": "Recreate"},
			"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{"example.com/x": "1"}}},
		},
	}
	server := &liveObjectServer{live: live}
	client, err := rest.UnversionedRESTClientFor(&rest.Config{
		Host: "http://api.invalid", Transport: handlerTransport{server},
		ContentConfig: rest.ContentConfig{NegotiatedSerializer: scheme.Codecs.WithoutConversion()},
	})
	if err != nil {
		t.Fatal(err)
	}
	resource := acceptedResource{client: client, path: "/apis/apps/v1/namespaces/app/deployments/web", namespaced: true}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "app"},
		"spec":     map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"a": "b"}}},
	}}
	if _, err := applyAdoption(context.Background(), resource, ref, source, source); err != nil {
		t.Fatal(err)
	}
	if len(server.applied) != 1 {
		t.Fatalf("want one apply, got %d", len(server.applied))
	}
	sent := &unstructured.Unstructured{Object: server.applied[0]}
	for path, want := range map[string]any{
		"spec.replicas": int64(3), "spec.minReadySeconds": int64(9), "spec.strategy.type": "Recreate",
	} {
		got, _, _ := unstructured.NestedFieldNoCopy(sent.Object, strings.Split(path, ".")...)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("apply dropped %s: got %v in %v", path, got, server.applied[0])
		}
	}
	got, _, _ := unstructured.NestedString(sent.Object, "spec", "template", "metadata", "annotations", "example.com/x")
	if got != "1" {
		t.Errorf("apply dropped the template annotation: %v", server.applied[0])
	}
}

// liveObjectServer serves one live object and records apply bodies.
type liveObjectServer struct {
	mu      sync.Mutex
	live    map[string]any
	applied []map[string]any
}

func (s *liveObjectServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPatch {
		body, _ := io.ReadAll(r.Body)
		var sent map[string]any
		_ = json.Unmarshal(body, &sent)
		s.applied = append(s.applied, sent)
	}
	_ = json.NewEncoder(w).Encode(s.live)
}
