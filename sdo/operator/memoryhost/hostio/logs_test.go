package hostio

import (
	"context"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestLiveLogsUseBoundedKubernetesOptions(t *testing.T) {
	var query string
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://synthetic.invalid", Transport: logTransport(func(r *http.Request) (*http.Response, error) {
		query = r.URL.RawQuery
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("synthetic logs"))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	view := NewLiveReadView(context.Background(), sdktest.Snapshot{NamespaceName: "app"}, ReadBackends{Client: client})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
	got, err := view.LogsWithOptions(ref, corev1.PodLogOptions{Container: "main", Previous: true})
	if err != nil || got != "synthetic logs" {
		t.Fatalf("logs = %q, %v", got, err)
	}
	if query != "container=main&limitBytes=65536&previous=true" {
		t.Fatalf("options lost: %s", query)
	}
	_, err = view.Logs(ref, "main", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if query != "container=main&limitBytes=65536&sinceSeconds=1" {
		t.Fatalf("duration not rounded up: %s", query)
	}
	for _, options := range []corev1.PodLogOptions{{}, {Container: "main", Follow: true}} {
		if _, err := view.LogsWithOptions(ref, options); err == nil {
			t.Fatal("unbounded or unspecified logs accepted")
		}
	}
}

type logTransport func(*http.Request) (*http.Response, error)

func (f logTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
