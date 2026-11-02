package runtime

import (
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

func countList(counter *atomic.Int32) func(ktesting.Action) (bool, runtime.Object, error) {
	return func(ktesting.Action) (bool, runtime.Object, error) {
		counter.Add(1)
		return false, nil, nil
	}
}

func awaitWatchEvent(t *testing.T, cache *KubernetesCache, want sdk.WatchKind) {
	t.Helper()
	<-cache.Notifications()
	got := cache.TakeEvents()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("unexpected watch events: got %#v, want %#v", got, want)
	}
}

func drainWatchEvents(cache *KubernetesCache) {
	select {
	case <-cache.Notifications():
	default:
	}
	cache.TakeEvents()
}

// Every watch the controller accepts at start must have an informer, so an
// accepted detector both loads and is notified of changes.
func TestEverySupportedWatchHasAnInformer(t *testing.T) {
	factory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
	for _, key := range core.SupportedWatches() {
		kind := key[strings.LastIndex(key, "/")+1:]
		if strings.HasPrefix(key, "sdo.dev/") || (sdk.ObservationTarget{Kind: kind}).Cluster() {
			// Synthetic traffic, reachability and metrics arrive without an
			// informer; cluster-scoped kinds are read outside the namespace cache.
			continue
		}
		if _, err := informerFor(factory, key); err != nil {
			t.Errorf("supported watch %s: %v", key, err)
		}
	}
}
