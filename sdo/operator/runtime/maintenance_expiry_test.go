package runtime

import (
	"context"
	"runtime"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"
)

func pausedWith(t *testing.T, data map[string]string) (MaintenanceWatcher, *clocktesting.FakeClock) {
	t.Helper()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: MaintenanceConfigMapName, Namespace: "sdo-system"}, Data: data,
	})
	clock := clocktesting.NewFakeClock(time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC))
	return MaintenanceWatcher{Clock: clock, Client: client, Namespace: "sdo-system", Name: MaintenanceConfigMapName}, clock
}

// A pause carries its own deadline: past it the controller reads the pause as
// resumed (and says so), so a declarer that never resumes cannot suspend
// detection forever. Before it, and with no deadline (a human pause), the
// pause holds.
func TestMaintenancePauseExpires(t *testing.T) {
	deadline := time.Date(2026, 11, 7, 0, 10, 0, 0, time.UTC)
	w, clock := pausedWith(t, map[string]string{"state": "paused", "generation": "upstream-advance-paused-1", maintenanceExpiresKey: deadline.Format(time.RFC3339Nano)})
	ctx := context.Background()
	if state, err := w.Read(ctx); err != nil || !state.Paused || state.Expired {
		t.Fatalf("before the deadline: %+v, %v", state, err)
	}
	clock.Step(10 * time.Minute)
	state, err := w.Read(ctx)
	if err != nil || state.Paused || !state.Expired || state.Generation != "upstream-advance-paused-1" {
		t.Fatalf("at the deadline: %+v, %v", state, err)
	}
	if state.Record()["maintenance_pause_expired"] != true || state.Record()["controller_maintenance"] != "active" {
		t.Fatalf("expiry is not logged: %v", state.Record())
	}

	human, _ := pausedWith(t, map[string]string{"state": "paused", "generation": "operator-redeploy"})
	if state, err := human.Read(ctx); err != nil || !state.Paused {
		t.Fatalf("a pause without a deadline expired: %+v, %v", state, err)
	}
	garbled, _ := pausedWith(t, map[string]string{"state": "paused", "generation": "g", maintenanceExpiresKey: "tomorrow"})
	if state, err := garbled.Read(ctx); err != nil || state.Paused || !state.Expired {
		t.Fatalf("an unreadable deadline must not pin the pause: %+v, %v", state, err)
	}
}

// The watch emits the resumed mode when the deadline passes, with the
// declaring side's Resume never having run.
func TestMaintenanceWatchResumesAtTheDeadline(t *testing.T) {
	deadline := time.Date(2026, 11, 7, 0, 10, 0, 0, time.UTC)
	w, clock := pausedWith(t, map[string]string{"state": "paused", "generation": "upstream-advance-paused-1", maintenanceExpiresKey: deadline.Format(time.RFC3339Nano)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initial, err := w.Read(ctx)
	if err != nil || !initial.Paused {
		t.Fatalf("initial = %+v, %v", initial, err)
	}
	changes := w.Watch(ctx, initial, time.Second, nil)
	// The ticker exists once the watch is polling; one tick before the
	// deadline changes nothing, one after it emits the resumed mode.
	for !clock.HasWaiters() {
		runtime.Gosched()
	}
	clock.Step(time.Minute)
	clock.Step(10 * time.Minute)
	state := <-changes
	if state.Paused || !state.Expired || !clock.Now().After(deadline) {
		t.Fatalf("emitted %+v at %v", state, clock.Now())
	}
}
