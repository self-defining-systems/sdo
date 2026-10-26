package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestRPXConvenienceReadsUseRecordedClientTransport(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("convenience read bypassed recorded API transport: Accept=%q", r.Header.Get("Accept"))
		}
		switch r.URL.Path {
		case "/api/v1/namespaces/app/pods/api/log":
			fmt.Fprint(w, "previous crash")
		case "/apis/metrics.k8s.io/v1beta1/namespaces/app/pods/api":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"apiVersion":"metrics.k8s.io/v1beta1","kind":"PodMetrics","metadata":{"name":"api","namespace":"app"},"timestamp":"2026-10-26T00:00:00Z","window":"30s","containers":[{"name":"main","usage":{"cpu":"125m","memory":"64Mi"}}]}`)
		case "/apis/metrics.k8s.io/v1beta1/nodes/worker":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"apiVersion":"metrics.k8s.io/v1beta1","kind":"NodeMetrics","metadata":{"name":"worker"},"timestamp":"2026-10-26T00:00:00Z","window":"30s","usage":{"cpu":"1","memory":"1Gi"}}`)
		default:
			t.Errorf("unexpected request %s", r.URL)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	v := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client, Context: context.Background()}
	pod := sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api"}
	if logs, err := v.LogsWithOptions(pod, corev1.PodLogOptions{Container: "main", Previous: true}); err != nil || logs != "previous crash" {
		t.Fatalf("logs=%q err=%v", logs, err)
	}
	if _, err := v.PodUsage(pod); err != nil {
		t.Fatal(err)
	}
	if _, err := v.NodeUsage(sdk.ObjectRef{Kind: "Node", Name: "worker"}); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
}
