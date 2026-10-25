package runtime

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func verboseCredentialLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	state := klog.CaptureState()
	t.Cleanup(state.Restore)
	flags := flag.NewFlagSet("credential-observation", flag.ContinueOnError)
	klog.InitFlags(flags)
	for key, value := range map[string]string{"v": "10", "logtostderr": "false"} {
		if err := flags.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	logs := &lockedBuffer{}
	klog.SetOutput(logs)
	return logs
}

func TestStateTrackerStartupNeverLogsCredentialBodies(t *testing.T) {
	const sentinel = "DBC2_SYNTHETIC_TRACKER_PASSWORD_9218"
	logs := verboseCredentialLogs(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/secrets") {
			t.Error("Secret read attempted")
			http.Error(w, "forbidden", 403)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/configmaps") && !strings.HasSuffix(r.URL.Path, "/pods") {
			http.Error(w, "forbidden", 403)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if strings.HasSuffix(r.URL.Path, "/configmaps") {
			_ = json.NewEncoder(w).Encode(corev1.ConfigMapList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMapList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "mongo-credentials", Namespace: "app", ResourceVersion: "1"}, Data: map[string]string{"auth": `{"opaque":"` + sentinel + `"}`}}}})
		} else {
			_ = json.NewEncoder(w).Encode(corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}, ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "mongo", Namespace: "app", ResourceVersion: "1"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo", Env: []corev1.EnvVar{{Name: "ROOT_PWD", Value: sentinel}}}}}}}})
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	tracker := NewStateTracker(StateTrackerConfig{Client: client, Namespace: "app"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tracker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	tracker.Stop()
	klog.Flush()
	if strings.Contains(logs.String(), sentinel) {
		t.Fatal("credential body leaked during production tracker startup")
	}
}
