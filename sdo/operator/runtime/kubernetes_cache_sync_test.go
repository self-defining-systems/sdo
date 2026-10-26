package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// One informer that cannot list must not hide behind a generic timeout: the
// sync error names the kind so a blind controller says why.
func TestKubernetesCacheSyncErrorNamesTheUnsyncedKind(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("persistentvolumeclaims is forbidden")
	})
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cache.Start(ctx)
	defer cache.Stop()

	err = cache.WaitForSync(ctx)
	var syncErr *InformerSyncError
	if !errors.As(err, &syncErr) {
		t.Fatalf("sync failure = %v, want *InformerSyncError", err)
	}
	if len(syncErr.Kinds) != 1 || syncErr.Kinds[0] != "v1/PersistentVolumeClaim" {
		t.Fatalf("unsynced kinds = %v, want only v1/PersistentVolumeClaim", syncErr.Kinds)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sync failure must keep its cause: %v", err)
	}
}

func TestKubernetesCacheSyncSucceedsWhenEveryInformerLists(t *testing.T) {
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cache.Start(ctx)
	defer cache.Stop()
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
}

// Every informer the cache can start has a resource the install grants can be
// derived from, so the grants ratchet covers the whole informer table.
func TestInformerResourcesCoverEveryInformer(t *testing.T) {
	for key := range informerResources {
		if _, err := informerFor(informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), key); err != nil {
			t.Errorf("informerResources lists %s but informerFor rejects it: %v", key, err)
		}
	}
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, grant := range cache.RequiredGrants() {
		if grant.Resource == "" {
			t.Errorf("informer %s has no resource", grant.Key)
		}
	}
}
