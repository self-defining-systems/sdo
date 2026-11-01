package runtime

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestMaintenanceWatcherUsesInjectedTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := clocktesting.NewFakeClock(time.Unix(123, 0))
		client := fake.NewSimpleClientset(maintenanceConfigMap("app", maintenanceStatePaused, "next"))
		watcher := MaintenanceWatcher{Client: client, Namespace: "app", Name: MaintenanceConfigMapName, Clock: clock}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		changes := watcher.Watch(ctx, MaintenanceState{}, time.Second, nil)
		synctest.Wait()
		clock.Step(time.Second)
		state := <-changes
		if !state.Paused || state.Generation != "next" {
			t.Fatalf("state %+v", state)
		}
	})
}

func TestLeaseExpiryUsesInjectedTimer(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Unix(456, 0))
	elector := NewLeaseElectorWithClock(fake.NewSimpleClientset(), "app", "leader", "worker", 2*time.Second, clock)
	acquired, err := elector.TryAcquireOrRenew(context.Background())
	if err != nil || !acquired {
		t.Fatalf("acquired %v, error %v", acquired, err)
	}
	guard, cancel, err := elector.GuardContext(context.Background())
	defer cancel()
	if err != nil {
		t.Fatal(err)
	}
	clock.Step(3 * time.Second)
	<-guard.Done()
	if guard.Err() == nil || elector.IsLeader() {
		t.Fatal("expired lease kept authority")
	}
}
