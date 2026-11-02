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
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: client, TrackDrift: true}, nil)
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
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
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
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, grant := range cache.RequiredGrants() {
		if grant.Resource == "" {
			t.Errorf("informer %s has no resource", grant.Key)
		}
	}
}

// A controller launched inside a baseline-preserving pause never starts its
// informers. Resume must start them rather than wait for a sync that cannot
// happen (h9.4 lane 50 row 005: resume timed out every minute, forever).
func TestKubernetesCacheObserveRunningStartsACacheThatWasNeverStarted(t *testing.T) {
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer cache.Stop()
	if err := cache.ObserveRunning(ctx, ctx); err != nil {
		t.Fatalf("resume of a never-started cache: %v", err)
	}
}

func TestKubernetesCacheObserveRunningKeepsRunningInformers(t *testing.T) {
	client := fake.NewSimpleClientset()
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: client, TrackDrift: true}, nil)
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
	before := len(client.Actions())
	if err := cache.ObserveRunning(ctx, ctx); err != nil {
		t.Fatal(err)
	}
	if after := len(client.Actions()); after != before {
		t.Fatalf("a preserved interval relisted: %d -> %d API calls", before, after)
	}
}

func TestKubernetesCacheObserveRelistsAfterStop(t *testing.T) {
	client := fake.NewSimpleClientset()
	cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cache.Start(ctx)
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	cache.Stop()
	before := len(client.Actions())
	if err := cache.Observe(ctx, ctx); err != nil {
		t.Fatal(err)
	}
	defer cache.Stop()
	if len(client.Actions()) == before {
		t.Fatal("a stopped cache was not relisted")
	}
}
