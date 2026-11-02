package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

// recordingTB captures what guardInformers reports instead of failing the
// real test.
type recordingTB struct {
	testing.TB
	cleanups []func()
	errors   []string
}

func (r *recordingTB) Helper()          {}
func (r *recordingTB) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }
func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, format)
}

func (r *recordingTB) runCleanups() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

func startRawInformer(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	factory := informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
	factory.Core().V1().Pods().Informer()
	factory.Start(ctx.Done())
	deadline := time.Now().Add(5 * time.Second)
	for informerGoroutines() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("informer never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return func() { cancel(); factory.Shutdown() }
}

// The guard is the class defense: it must fail an owner whose Stop leaves a
// reflector running, and stay silent when Stop ends it.
func TestGuardInformersFailsOwnerThatLeaksAndPassesOneThatStops(t *testing.T) {
	defer func(grace time.Duration) { informerStopGrace = grace }(informerStopGrace)
	informerStopGrace = 200 * time.Millisecond

	leaky := &recordingTB{TB: t}
	guardInformers(leaky, "leaky owner", func() {})
	stop := startRawInformer(t)
	leaky.runCleanups()
	if len(leaky.errors) != 1 {
		t.Fatalf("a leaked reflector produced %d failures, want 1", len(leaky.errors))
	}
	stop() // end the deliberate leak so later tests start clean
	requireNoInformerGoroutines(t, "the self-test cleanup")

	clean := &recordingTB{TB: t}
	guardInformers(clean, "clean owner", func() {})
	stopClean := startRawInformer(t)
	clean.cleanups = nil
	guardInformers(clean, "clean owner", stopClean)
	clean.runCleanups()
	if len(clean.errors) != 0 {
		t.Fatalf("an owner that stopped its informers was failed: %v", clean.errors)
	}
}
