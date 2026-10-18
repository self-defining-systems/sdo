package runtime

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var configMapsResource = corev1.SchemeGroupVersion.WithResource("configmaps")

// enforceConfigMapResourceVersions gives the fake clientset the API server's
// optimistic concurrency for ConfigMaps: every write assigns a new
// resourceVersion and an Update carrying a stale one is rejected with 409.
func enforceConfigMapResourceVersions(client *fake.Clientset) {
	var mu sync.Mutex
	next := 100
	client.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		object := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
		next++
		object.ResourceVersion = strconv.Itoa(next)
		if err := client.Tracker().Create(configMapsResource, object, object.Namespace); err != nil {
			return true, nil, err
		}
		return true, object.DeepCopy(), nil
	})
	client.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		object := action.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
		current, err := client.Tracker().Get(configMapsResource, object.Namespace, object.Name)
		if err != nil {
			return true, nil, err
		}
		if object.ResourceVersion != current.(*corev1.ConfigMap).ResourceVersion {
			return true, nil, apierrors.NewConflict(
				configMapsResource.GroupResource(), object.Name, errors.New("resourceVersion mismatch"),
			)
		}
		next++
		object.ResourceVersion = strconv.Itoa(next)
		if err := client.Tracker().Update(configMapsResource, object, object.Namespace); err != nil {
			return true, nil, err
		}
		return true, object.DeepCopy(), nil
	})
}

func verbs(client *fake.Clientset) []string {
	result := make([]string, 0)
	for _, action := range client.Actions() {
		result = append(result, action.GetVerb())
	}
	return result
}

func equalVerbs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func historyState(detectorIDs ...string) RuntimeState {
	state := RuntimeState{Version: RuntimeStateVersion}
	for _, detectorID := range detectorIDs {
		state.History = append(state.History, DetectorEvaluation{DetectorID: detectorID})
	}
	return state
}

func TestConfigMapStateStoreSkipsUnchangedStateWithoutAPICalls(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	store := NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, historyState("health"), "")
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	client.ClearActions()

	again, err := store.Save(ctx, historyState("health"), revision)
	if err != nil {
		t.Fatalf("save unchanged state: %v", err)
	}
	if again != revision {
		t.Fatalf("unchanged save moved revision %q to %q", revision, again)
	}
	if got := verbs(client); len(got) != 0 {
		t.Fatalf("unchanged state made API calls: %v", got)
	}
}

func TestConfigMapStateStoreSkipsStateUnchangedSinceLoad(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	if _, err := NewConfigMapStateStore(client, "demo", "state").Save(ctx, historyState("health"), ""); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	store := NewConfigMapStateStore(client, "demo", "state")
	loaded, revision, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	client.ClearActions()
	if _, err := store.Save(ctx, loaded, revision); err != nil {
		t.Fatalf("save loaded state: %v", err)
	}
	if got := verbs(client); len(got) != 0 {
		t.Fatalf("state unchanged since load made API calls: %v", got)
	}
}

func TestConfigMapStateStoreWritesChangedStateWithOneUpdate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	store := NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, historyState("health"), "")
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	client.ClearActions()

	next, err := store.Save(ctx, historyState("health", "incident"), revision)
	if err != nil {
		t.Fatalf("save changed state: %v", err)
	}
	if got := verbs(client); !equalVerbs(got, "update") {
		t.Fatalf("changed state used %v, want a single update", got)
	}
	loaded, loadedRevision, err := NewConfigMapStateStore(client, "demo", "state").Load(ctx)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if loadedRevision != next || next == revision || len(loaded.History) != 2 {
		t.Fatalf("durable state %#v revision %q, want two entries at %q", loaded, loadedRevision, next)
	}
}

func TestConfigMapStateStoreStaleWriterConflictsUnderResourceVersionCAS(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	if _, err := NewConfigMapStateStore(client, "demo", "state").Save(ctx, historyState("health"), ""); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	leader := NewConfigMapStateStore(client, "demo", "state")
	stale := NewConfigMapStateStore(client, "demo", "state")
	_, leaderRevision, err := leader.Load(ctx)
	if err != nil {
		t.Fatalf("leader load: %v", err)
	}
	_, staleRevision, err := stale.Load(ctx)
	if err != nil {
		t.Fatalf("stale load: %v", err)
	}
	if _, err := leader.Save(ctx, historyState("leader"), leaderRevision); err != nil {
		t.Fatalf("leader save: %v", err)
	}
	if _, err := stale.Save(ctx, historyState("stale"), staleRevision); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("stale writer: got %v, want ErrStateConflict", err)
	}
	durable, _, err := NewConfigMapStateStore(client, "demo", "state").Load(ctx)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if len(durable.History) != 1 || durable.History[0].DetectorID != "leader" {
		t.Fatalf("stale writer overwrote leader state: %#v", durable.History)
	}
}

func TestConfigMapStateStoreRetriesMetadataOnlyConflictWithGetAndUpdate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	store := NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, historyState("health"), "")
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	// An operator labels the ConfigMap: resourceVersion moves, the state
	// revision does not.
	labeled, err := client.CoreV1().ConfigMaps("demo").Get(ctx, "state", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	labeled.Labels = map[string]string{"team": "sre"}
	if _, err := client.CoreV1().ConfigMaps("demo").Update(ctx, labeled, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("label state: %v", err)
	}
	client.ClearActions()

	if _, err := store.Save(ctx, historyState("health", "incident"), revision); err != nil {
		t.Fatalf("save after metadata-only change: %v", err)
	}
	if got := verbs(client); !equalVerbs(got, "update", "get", "update") {
		t.Fatalf("conflict fallback used %v, want update, get, update", got)
	}
	current, err := client.CoreV1().ConfigMaps("demo").Get(ctx, "state", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if current.Labels["team"] != "sre" {
		t.Fatalf("state write dropped operator metadata: %#v", current.Labels)
	}
}

func TestConfigMapStateStoreDoesNotTreatFailedWriteAsPersisted(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	enforceConfigMapResourceVersions(client)
	store := NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, historyState("health"), "")
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	failNext := true
	client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		if failNext {
			failNext = false
			return true, nil, apierrors.NewInternalError(errors.New("etcd timeout"))
		}
		return false, nil, nil
	})
	changed := historyState("health", "incident")
	if _, err := store.Save(ctx, changed, revision); err == nil {
		t.Fatal("failed update reported success")
	}
	client.ClearActions()

	if _, err := store.Save(ctx, changed, revision); err != nil {
		t.Fatalf("retry changed state: %v", err)
	}
	if got := verbs(client); len(got) == 0 {
		t.Fatal("retry after a failed write skipped the write as unchanged")
	}
	durable, _, err := NewConfigMapStateStore(client, "demo", "state").Load(ctx)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if len(durable.History) != 2 {
		t.Fatalf("retried state is not durable: %#v", durable.History)
	}
}
