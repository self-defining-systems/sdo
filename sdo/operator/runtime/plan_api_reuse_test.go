package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk/program"
)

func TestKubernetesReadSharesSuppliedClientTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()
	var builds atomic.Int32
	config := &rest.Config{Host: server.URL, WrapTransport: func(rt http.RoundTripper) http.RoundTripper { builds.Add(1); return rt }}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	plan := TypedPlanClient{Client: client, ExecConfig: config}
	for i := 0; i < 8; i++ {
		if _, err := plan.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "GET", Path: "/api/v1/pods"}); err != nil {
			t.Fatal(err)
		}
	}
	if builds.Load() != 1 {
		t.Fatalf("constructed %d authenticated transports for one process, want 1", builds.Load())
	}
}

func TestReadClientMayObserveControlObjectsWithoutWriteAuthority(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"status","namespace":"sdo"}}`))
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	plan := TypedPlanClient{Client: client, ExecConfig: config, ProtectedNamespace: "sdo"}
	if _, err := plan.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/sdo/configmaps/status"}); err != nil {
		t.Fatalf("control object observation denied: %v", err)
	}
	if _, err := plan.KubernetesRequest(context.Background(), program.KubernetesRequest{Method: "PATCH", Path: "/api/v1/namespaces/sdo/configmaps/status"}); err == nil {
		t.Fatal("control object mutation allowed")
	}
}
