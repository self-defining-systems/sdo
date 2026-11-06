package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
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
