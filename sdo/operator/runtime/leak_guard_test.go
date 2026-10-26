package runtime

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// Informers read process-wide state (klog verbosity) from their reflector
// goroutines, so one that outlives its test races with whatever runs next and
// the failure lands on an innocent test. Tests therefore build StateTrackers
// and KubernetesCaches only through the helpers below, which stop the owner at
// cleanup and fail that very test, by name, if a reflector survives. The
// ratchet in leak_guard_ratchet_test.go forbids bypassing them, and TestMain
// catches informers started any other way.
//
// Attribution counts process-wide reflectors, so a test using these helpers
// must not run in parallel with another that starts informers.

const informerStopGrace = 5 * time.Second

// informerGoroutines counts live client-go reflector goroutines.
func informerGoroutines() int {
	buffer := make([]byte, 1<<22)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			buffer = buffer[:n]
			break
		}
		buffer = make([]byte, 2*len(buffer))
	}
	return strings.Count(string(buffer), "k8s.io/client-go/tools/cache.(*Reflector).Run")
}

// awaitInformerGoroutines polls until at most baseline reflectors run, and
// returns the final count.
func awaitInformerGoroutines(baseline int, grace time.Duration) int {
	deadline := time.Now().Add(grace)
	for {
		count := informerGoroutines()
		if count <= baseline || time.Now().After(deadline) {
			return count
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireNoInformerGoroutines(t testing.TB, owner string) {
	t.Helper()
	if count := informerGoroutines(); count != 0 {
		t.Fatalf("%s left %d informer goroutines running; every started informer must be stopped and awaited", owner, count)
	}
}

// guardInformers fails t if more reflectors run after stop than when it was
// called. Measured against the starting count, so one leaking test is blamed
// once instead of every test after it.
func guardInformers(t testing.TB, owner string, stop func()) {
	t.Helper()
	baseline := informerGoroutines()
	t.Cleanup(func() {
		stop()
		if count := awaitInformerGoroutines(baseline, informerStopGrace); count > baseline {
			t.Errorf("%s left %d informer goroutines running after Stop (%d before the test); stop and await every informer it starts", owner, count-baseline, baseline)
		}
	})
}

func newTrackerForTest(t testing.TB, config StateTrackerConfig) *StateTracker {
	t.Helper()
	tracker := NewStateTracker(config)
	guardInformers(t, "StateTracker", tracker.Stop)
	return tracker
}

func newCacheForTest(t testing.TB, config KubernetesCacheConfig, detectors []sdk.Detector) (*KubernetesCache, error) {
	t.Helper()
	cache, err := NewKubernetesCache(config, detectors)
	if err != nil {
		return nil, err
	}
	guardInformers(t, "KubernetesCache", cache.Stop)
	return cache, nil
}
