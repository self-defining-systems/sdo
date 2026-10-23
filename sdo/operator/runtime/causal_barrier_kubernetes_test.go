package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
)

func TestCausalBarrierKubernetesReaderUsesMostRecentNamespacedLists(t *testing.T) {
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		if r.Method != "GET" || r.URL.Path != "/api/v1/namespaces/app/services" || r.URL.Query().Get("resourceVersion") != "" || r.URL.Query().Get("limit") != "1001" {
			t.Errorf("unexpected direct read: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"ServiceList","metadata":{"resourceVersion":"opaque-current"},"items":[]}`)
	}))
	defer server.Close()
	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Version: "v1"}})
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Service"}, meta.RESTScopeNamespace)
	reader := KubernetesCurrentReader{Client: KubernetesPlanClient{Client: client, Mapper: mapper}}
	result, err := reader.ListCurrent(context.Background(), sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"})
	if err != nil || !result.Complete || result.ResourceVersion != "opaque-current" || reads != 1 {
		t.Fatalf("current read: %+v %v", result, err)
	}
	for _, kind := range []sdk.WatchKind{{APIVersion: "v1", Kind: "Secret", Namespace: "app"}, {APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Unwatched", Namespace: "app"}} {
		if _, err := reader.ListCurrent(context.Background(), kind); err == nil {
			t.Fatal("unsupported/unnamespaced read was permitted")
		}
	}
	if reads != 1 {
		t.Fatal("unsupported kinds fetched content")
	}
}

func TestCausalBarrierTypedReaderPreservesCurrentListContract(t *testing.T) {
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
		if r.URL.Path != "/api/v1/namespaces/app/services" || r.URL.Query().Get("resourceVersion") != "" || r.URL.Query().Get("limit") != "1001" {
			t.Errorf("unexpected typed list: %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"ServiceList","metadata":{"resourceVersion":"current","continue":"next-page"},"items":[]}`)
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	reader := KubernetesCurrentReader{TypedClient: client}
	result, err := reader.ListCurrent(context.Background(), sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"})
	if err != nil || result.Complete || result.ResourceVersion != "current" || reads != 1 {
		t.Fatalf("pagination falsely proved collection closure: %+v %v", result, err)
	}
	if _, err := reader.ListCurrent(context.Background(), sdk.WatchKind{APIVersion: "v1", Kind: "Secret", Namespace: "app"}); err == nil || reads != 1 {
		t.Fatal("typed reader granted a Secret read")
	}
}
