package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
)

// conflictingServer is a one-object API server whose resourceVersion moves on
// every write by another controller; a conditional apply naming a stale
// resourceVersion is refused with 409 like the real API server.
type conflictingServer struct {
	mu         sync.Mutex
	version    int
	targetPort int
	// bumpsBeforeApply is how many controller writes land between the caller's
	// read and each of its conditional applies, for the first N applies.
	bumpsBeforeApply int
	applies          int
}

func (s *conflictingServer) object() map[string]any {
	return map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"name": "frontend", "namespace": "app", "uid": "u1", "resourceVersion": strconv.Itoa(s.version)},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": 8080, "targetPort": s.targetPort}}},
	}
}

func (s *conflictingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPatch {
		body, _ := io.ReadAll(r.Body)
		var sent map[string]any
		_ = json.Unmarshal(body, &sent)
		s.applies++
		if s.applies <= s.bumpsBeforeApply {
			s.version++ // a controller wrote status after the caller's read
		}
		metadata, _ := sent["metadata"].(map[string]any)
		if rv, ok := metadata["resourceVersion"].(string); ok && rv != strconv.Itoa(s.version) {
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","code":409,"message":"the object has been modified; please apply your changes to the latest version and try again"}`)
			return
		}
		if spec, ok := sent["spec"].(map[string]any); ok && r.URL.Query().Get("dryRun") == "" {
			ports, _ := spec["ports"].([]any)
			if len(ports) > 0 {
				s.targetPort = int(ports[0].(map[string]any)["targetPort"].(float64))
			}
		}
		s.version++
	}
	_ = json.NewEncoder(w).Encode(s.object())
}

// handlerTransport serves requests in process: no socket, so no transport to inject.
type handlerTransport struct{ handler http.Handler }

func (h handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, r)
	return recorder.Result(), nil
}

func conflictingResource(t *testing.T, server *conflictingServer) acceptedResource {
	t.Helper()
	client, err := rest.UnversionedRESTClientFor(&rest.Config{
		Host:          "http://api.invalid",
		Transport:     handlerTransport{server},
		ContentConfig: rest.ContentConfig{NegotiatedSerializer: scheme.Codecs.WithoutConversion()},
	})
	if err != nil {
		t.Fatal(err)
	}
	return acceptedResource{client: client, path: "/api/v1/namespaces/app/services/frontend", namespaced: true}
}

func declaredService(targetPort int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"name": "frontend", "namespace": "app"},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": int64(8080), "targetPort": int64(targetPort)}}},
	}}
}

var frontendRef = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "frontend"}

// A write by another controller between the scan's read and the adoption's
// conditional apply must not leave the object unadopted until the next backoff
// pass: the repeat incident would meet a missing acceptance record.
func TestAdoptionRetriesAConflictWhenTheDeclaredFieldsStillMatch(t *testing.T) {
	server := &conflictingServer{version: 5, targetPort: 8080, bumpsBeforeApply: 2}
	resource := conflictingResource(t, server)
	source := declaredService(8080)
	live, err := resource.get(context.Background(), frontendRef)
	if err != nil {
		t.Fatal(err)
	}
	conditional := source.DeepCopy()
	conditional.SetUID(live.GetUID())
	conditional.SetResourceVersion(live.GetResourceVersion())

	if _, err := applyAdoption(context.Background(), resource, frontendRef, conditional, source); err != nil {
		t.Fatalf("a benign conflict must be retried on the fresh resourceVersion: %v", err)
	}
	if server.applies < 3 {
		t.Fatalf("want a retry after each conflict, saw %d applies", server.applies)
	}
}

// A conflict caused by a change to a declared field is drift to classify, never
// something the adoption may overwrite.
func TestAdoptionDoesNotOverwriteAConcurrentChangeToADeclaredField(t *testing.T) {
	server := &conflictingServer{version: 5, targetPort: 9999, bumpsBeforeApply: 1}
	resource := conflictingResource(t, server)
	source := declaredService(8080)
	conditional := source.DeepCopy()
	conditional.SetUID("u1")
	conditional.SetResourceVersion("5")

	_, err := applyAdoption(context.Background(), resource, frontendRef, conditional, source)
	if err == nil {
		t.Fatal("a conflict over a changed declared field must be returned, not retried over")
	}
	if server.targetPort != 9999 {
		t.Fatalf("adoption overwrote the concurrent change: targetPort %d", server.targetPort)
	}
}
