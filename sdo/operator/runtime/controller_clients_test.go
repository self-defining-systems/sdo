package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// Exercise production client construction and the real state producer/consumer
// over HTTP, with the observation bucket deliberately exhausted. No cluster or
// credentials are used. Reusing the observation client makes Save fail before
// the HTTP request because client-go cannot acquire a token by the deadline.
func TestControllerStatePersistenceCannotBeStarvedByObservation(t *testing.T) {
	var stored *corev1.ConfigMap
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/demo/pods":
			_ = json.NewEncoder(w).Encode(&corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}})
		case "/api/v1/namespaces/control/configmaps/state":
			if stored == nil {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(&metav1.Status{Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
				return
			}
			_ = json.NewEncoder(w).Encode(stored)
		case "/api/v1/namespaces/control/configmaps":
			if r.Method != http.MethodPost {
				t.Errorf("unexpected method %s", r.Method)
				w.WriteHeader(405)
				return
			}
			stored = &corev1.ConfigMap{}
			if err := json.NewDecoder(r.Body).Decode(stored); err != nil {
				t.Error(err)
			}
			stored.APIVersion, stored.Kind, stored.ResourceVersion = "v1", "ConfigMap", "1"
			_ = json.NewEncoder(w).Encode(stored)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "kubeconfig")
	content := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: synthetic\n  cluster:\n    server: %s\ncontexts:\n- name: synthetic\n  context:\n    cluster: synthetic\ncurrent-context: synthetic\n", server.URL)
	if err := os.WriteFile(config, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBECONFIG", config)
	t.Setenv("SDO_KUBE_API_QPS", "0.01")
	t.Setenv("SDO_KUBE_API_BURST", "3")
	provider, _, stateClient, err := controllerKubernetesClients("demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := provider.Client.CoreV1().Pods("demo").List(context.Background(), metav1.ListOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := lifecycleio.NewConfigMapStateStore(stateClient, "control", "state")
	revision, err := store.Save(ctx, historyState("health"), "")
	if err != nil {
		t.Fatalf("observation traffic starved durable state: %v", err)
	}
	loaded, got, err := store.Load(ctx)
	if err != nil || got != revision || len(loaded.GetHistory()) != 1 {
		t.Fatalf("durable readback: revision=%q, state=%v, err=%v", got, loaded, err)
	}
	// All three state tokens are now spent. Unchanged checkpoints must remain
	// request-free rather than exhausting even the dedicated state budget.
	for range 20 {
		if _, err := store.Save(ctx, loaded, revision); err != nil {
			t.Fatalf("unchanged checkpoint consumed state API budget: %v", err)
		}
	}
}
