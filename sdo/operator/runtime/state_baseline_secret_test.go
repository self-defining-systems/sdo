package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/core"
)

type secretMetadataRequest struct {
	path, accept, method string
	watch                bool
}
type secretMetadataAPI struct {
	mu       sync.Mutex
	requests []secretMetadataRequest
	events   chan watch.Event
}

// The canary stays server-side. Only metadata is sent on valid requests; error
// assertions never print response bodies or Secret objects.
func secretAPITracker(t *testing.T, secret *corev1.Secret, status int, fullReply bool) (*StateTracker, *secretMetadataAPI) {
	t.Helper()
	api := &secretMetadataAPI{events: make(chan watch.Event, 10)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isWatch := r.URL.Query().Get("watch") == "true"
		api.mu.Lock()
		api.requests = append(api.requests, secretMetadataRequest{r.URL.Path, r.Header.Get("Accept"), r.Method, isWatch})
		api.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		if isWatch {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			for {
				select {
				case event := <-api.events:
					_ = json.NewEncoder(w).Encode(map[string]any{"type": event.Type, "object": event.Object})
					w.(http.Flusher).Flush()
				case <-r.Context().Done():
					return
				}
			}
		}
		if fullReply {
			_ = json.NewEncoder(w).Encode(&corev1.SecretList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "SecretList"}, Items: []corev1.Secret{*secret}})
			return
		}
		items := []metav1.PartialObjectMetadata{}
		if secret != nil {
			items = append(items, metav1.PartialObjectMetadata{ObjectMeta: secret.ObjectMeta})
		}
		_ = json.NewEncoder(w).Encode(&metav1.PartialObjectMetadataList{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadataList"}, ListMeta: metav1.ListMeta{ResourceVersion: "10"}, Items: items})
	}))
	t.Cleanup(server.Close)
	// Exercise the production metadata factory and its metadata-only decoder.
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	content := "apiVersion: v1\nkind: Config\nclusters:\n- name: test\n  cluster:\n    server: " + server.URL + "\ncontexts:\n- name: test\n  context:\n    cluster: test\ncurrent-context: test\n"
	if err := os.WriteFile(kubeconfig, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	metadataClient, err := core.NewKubernetesMetadataClient()
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	// Any use of the typed Secret client violates the boundary, even if a
	// full-object read were discarded before snapshotting.
	t.Cleanup(func() {
		for _, action := range client.Actions() {
			if action.GetResource().Resource == "secrets" {
				t.Error("baseline used the typed Secret client")
			}
		}
	})
	tracker := newTrackerForTest(t, StateTrackerConfig{Client: client, MetadataClient: metadataClient, Namespace: hotel})
	t.Cleanup(tracker.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tracker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return tracker, api
}

func (api *secretMetadataAPI) assertRequests(t *testing.T, needWatch bool) {
	t.Helper()
	api.mu.Lock()
	defer api.mu.Unlock()
	listed, watched := false, false
	for _, request := range api.requests {
		expected := "PartialObjectMetadataList"
		if request.watch {
			expected = "PartialObjectMetadata"
		}
		for _, accept := range strings.Split(request.accept, ",") {
			_, params, err := mime.ParseMediaType(accept)
			if err != nil || params["as"] != expected || params["g"] != "meta.k8s.io" || params["v"] != "v1" {
				t.Error("Secret request permits a full-object fallback")
			}
		}
		if request.path != "/api/v1/namespaces/"+hotel+"/secrets" || request.method != http.MethodGet {
			t.Error("unexpected Secret request path/method")
		}
		listed = listed || !request.watch
		watched = watched || request.watch
	}
	if !listed || (needWatch && !watched) {
		t.Error("missing Secret metadata list/watch")
	}
}

func (api *secretMetadataAPI) waitForWatch(t *testing.T) {
	t.Helper()
	eventually(t, "Secret metadata watch", func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		for _, r := range api.requests {
			if r.watch {
				return true
			}
		}
		return false
	})
}

func TestStateBaselineNeverReceivesOrStoresSecretData(t *testing.T) {
	canary := []byte("synthetic-secret-boundary-canary")
	encoded := base64.StdEncoding.EncodeToString(canary)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "credentials", Namespace: hotel, UID: "secret-uid", ResourceVersion: "10",
		Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": encoded, "other": string(canary)},
		Labels:      map[string]string{"untrusted": string(canary), "app.kubernetes.io/managed-by": string(canary)},
	}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"password": canary}, StringData: map[string]string{"token": string(canary)}}
	tracker, api := secretAPITracker(t, secret, http.StatusOK, false)
	api.waitForWatch(t)
	updated := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}, ObjectMeta: secret.ObjectMeta}
	updated.ResourceVersion = "11"
	api.events <- watch.Event{Type: watch.Modified, Object: updated}
	eventually(t, "canary metadata watch update", func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		return tracker.snapshotLocked(time.Now()).objects["Secret/credentials"]["resourceVersion"] == "11"
	})
	now := time.Now()
	tracker.Observe(now, true)
	tracker.Observe(now.Add(time.Second), true)
	api.assertRequests(t, true)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	stored, err := json.Marshal([]any{tracker.baseline.objects, tracker.candidate.objects, tracker.snapshotLocked(now).objects, tracker.informers["Secret"].GetStore().List()})
	state := tracker.baseline.objects["Secret/credentials"]
	if err != nil {
		t.Fatal("cannot encode baseline/cache")
	}
	for _, excluded := range []string{string(canary), encoded, digest(canary), "data[password]", "stringData", "annotations", "untrusted"} {
		if strings.Contains(string(stored), excluded) {
			t.Error("excluded Secret fields or a derived digest reached the baseline/cache")
		}
	}
	if !reflect.DeepEqual(state, objectState{"uid": "secret-uid", "resourceVersion": "11"}) {
		t.Error("baseline must store only Secret identity/version")
	}
}

func TestStateBaselineSecretMetadataDrift(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: hotel, UID: "uid-1", ResourceVersion: "10"}}
	tracker, api := secretAPITracker(t, secret, http.StatusOK, false)
	api.waitForWatch(t)
	now := time.Now()
	tracker.Observe(now, true)
	if changes := tracker.Changes(now); len(changes.Changes) != 0 {
		t.Fatal("initial metadata baseline changed")
	}
	// Content changes are visible only through the API's opaque resourceVersion.
	updated := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}, ObjectMeta: secret.ObjectMeta}
	updated.ResourceVersion = "opaque-next"
	api.events <- watch.Event{Type: watch.Modified, Object: updated}
	eventually(t, "resourceVersion drift", func() bool {
		change := findChange(tracker.Changes(now), "Secret", "credentials")
		return change != nil && field(change, "resourceVersion") != nil
	})
	change := findChange(tracker.Changes(now), "Secret", "credentials")
	if len(change.Fields) != 1 || field(change, "resourceVersion").Before != "10" || field(change, "resourceVersion").After != "opaque-next" {
		t.Fatal("Secret content drift must report only resourceVersion")
	}
	replacement := updated.DeepCopy()
	replacement.UID, replacement.ResourceVersion = "uid-2", "replacement"
	api.events <- watch.Event{Type: watch.Modified, Object: replacement}
	eventually(t, "Secret recreation identity", func() bool {
		change := findChange(tracker.Changes(now), "Secret", "credentials")
		return change != nil && field(change, "uid") != nil
	})
	api.events <- watch.Event{Type: watch.Deleted, Object: replacement}
	eventually(t, "Secret removal", func() bool {
		change := findChange(tracker.Changes(now), "Secret", "credentials")
		return change != nil && change.Change == StateChangeRemoved
	})
	added := updated.DeepCopy()
	added.Name = "new-credentials"
	api.events <- watch.Event{Type: watch.Added, Object: added}
	eventually(t, "Secret addition", func() bool {
		change := findChange(tracker.Changes(now), "Secret", added.Name)
		return change != nil && change.Change == StateChangeAdded && len(change.Fields) == 2
	})
	api.assertRequests(t, true)
}

func TestStateBaselineSecretMetadataFailsClosed(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotAcceptable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			tracker, api := secretAPITracker(t, nil, status, false)
			tracker.Observe(time.Now(), true)
			if !reflect.DeepEqual(tracker.UnobservedKinds(), []string{"Secret"}) {
				t.Fatal("unavailable metadata must mark Secret unobserved")
			}
			api.assertRequests(t, false)
		})
	}
	t.Run("unexpected full object", func(t *testing.T) {
		tracker, api := secretAPITracker(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: hotel}}, http.StatusOK, true)
		if !reflect.DeepEqual(tracker.UnobservedKinds(), []string{"Secret"}) {
			t.Fatal("full-object response must be rejected")
		}
		api.assertRequests(t, false)
	})
}
