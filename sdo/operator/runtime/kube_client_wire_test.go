package runtime

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestRecordedClientPreviousAndEmptyLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("previous") != "true" {
			t.Error("previous log option lost")
		}
		w.Header().Set("Content-Type", "text/plain")
		if r.URL.Query().Get("container") != "empty" {
			io.WriteString(w, "previous crash\nsecond line\n")
		}
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	upstream, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	captured := &sdktest.RecordingReadView{ReadView: LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: upstream, DatabaseExecConfig: config}}
	for _, container := range []string{"api", "empty"} {
		options := &corev1.PodLogOptions{Container: container, Previous: true}
		client, err := program.Kubernetes(captured)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := client.CoreV1().Pods("app").GetLogs("api", options).DoRaw(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := "previous crash\nsecond line\n"
		if container == "empty" {
			want = ""
		}
		if string(raw) != want {
			t.Fatalf("log bytes changed: %q want %q", raw, want)
		}
		replay, err := program.Kubernetes(&sdktest.ReplayReadView{Calls: captured.Calls})
		if err != nil {
			t.Fatal(err)
		}
		offline, err := replay.CoreV1().Pods("app").GetLogs("api", options).DoRaw(context.Background())
		if err != nil || string(offline) != want {
			t.Fatalf("offline log bytes: %q %v", offline, err)
		}
	}
}
func TestRecordedClientSecretReadsNeverReachTransport(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"data":{"token":"KC_SECRET_CANARY"}}`)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	upstream, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	captured := &sdktest.RecordingReadView{ReadView: LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: upstream, DatabaseExecConfig: config}}
	client, err := program.Kubernetes(captured)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Secrets("app").Get(context.Background(), "token", metav1.GetOptions{}); err == nil {
		t.Fatal("Secret read succeeded")
	}
	if _, err := client.CoreV1().Secrets("app").List(context.Background(), metav1.ListOptions{}); err == nil {
		t.Fatal("Secret list succeeded")
	}
	if calls.Load() != 0 || len(captured.Calls) != 0 {
		t.Fatal("Secret endpoint reached recording or API")
	}
}
