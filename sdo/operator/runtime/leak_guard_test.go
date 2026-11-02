package runtime

import (
	"runtime"
	"strings"
	"testing"
	"time"

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

// informerStopGrace is a variable only so the guard's own test can shorten it.
var informerStopGrace = 5 * time.Second

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

func requireNoInformerGoroutines(t testing.TB, owner string) {
	t.Helper()
	if count := informerGoroutines(); count != 0 {
		t.Fatalf("%s left %d informer goroutines running; every started informer must be stopped and awaited", owner, count)
	}
}

