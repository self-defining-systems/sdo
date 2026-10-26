package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
)

// forbiddenServer answers every request with a JSON Status, as the API server
// does for an RBAC refusal.
func forbiddenServer(t *testing.T) *acceptedResource {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden,
			Message: `serviceaccounts "grafana" is forbidden: User "system:serviceaccount:sdo-system:sdo-controller" cannot patch resource "serviceaccounts" in API group "" in the namespace "shop"`}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(status)
	}))
	t.Cleanup(server.Close)
	client, err := rest.UnversionedRESTClientFor(&rest.Config{Host: server.URL, ContentConfig: rest.ContentConfig{NegotiatedSerializer: scheme.Codecs.WithoutConversion()}})
	if err != nil {
		t.Fatal(err)
	}
	return &acceptedResource{client: client, path: "/api/v1/namespaces/shop/serviceaccounts/grafana", namespaced: true}
}

// The API server's Status survives every accepted-source request, so a refusal
// names its code, reason and message instead of the generic "unknown".
func TestAcceptedSourceRequestsKeepServerStatus(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "shop", Name: "grafana"}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "grafana", "namespace": "shop"}}}
	resource := forbiddenServer(t)
	calls := map[string]func() error{
		"get":               func() error { _, err := resource.get(context.Background(), ref); return err },
		"dry-run apply":     func() error { _, err := resource.apply(context.Background(), ref, source, true); return err },
		"record acceptance": func() error { return resource.recordAcceptance(context.Background(), ref, "{}") },
	}
	for name, call := range calls {
		err := call()
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
		if !apierrors.IsForbidden(err) {
			t.Errorf("%s: want a typed Forbidden status error, got %T: %v", name, err, err)
		}
		if !strings.Contains(err.Error(), `cannot patch resource "serviceaccounts"`) {
			t.Errorf("%s: server message lost: %v", name, err)
		}
	}
}

// Result.Raw drops the server Status; no production request in this package
// may end in Raw() outside bodyOf, which reads Result.Error first.
func TestProductionRequestsNeverReadRawBeforeError(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(body), "\n") {
			if (strings.Contains(line, ".Raw()") || strings.Contains(line, ".DoRaw(")) && !strings.Contains(line, "return result.Raw()") {
				t.Errorf("%s:%d reads a request body without Result.Error: %s", file, number+1, strings.TrimSpace(line))
			}
		}
	}
}
