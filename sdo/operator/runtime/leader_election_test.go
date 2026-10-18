package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestLeaseElectorSingleLeaderReleaseAndHandoff(t *testing.T) {
	client := fake.NewSimpleClientset()
	first := NewLeaseElector(client, "demo", "sdo", "first", 15*time.Second)
	second := NewLeaseElector(client, "demo", "sdo", "second", 15*time.Second)
	now := time.Unix(100, 0)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	ctx := context.Background()

	if leader, err := first.TryAcquireOrRenew(ctx); err != nil || !leader {
		t.Fatalf("first failed to acquire: leader=%v err=%v", leader, err)
	}
	if !first.IsLeader() {
		t.Fatal("elector did not expose acquired leadership")
	}
	if leader, err := second.TryAcquireOrRenew(ctx); err != nil || leader {
		t.Fatalf("standby acquired live Lease: leader=%v err=%v", leader, err)
	}
	if second.IsLeader() {
		t.Fatal("standby reported active leadership")
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if first.IsLeader() {
		t.Fatal("released elector retained active leadership")
	}
	if leader, err := second.TryAcquireOrRenew(ctx); err != nil || !leader {
		t.Fatalf("successor failed to acquire: leader=%v err=%v", leader, err)
	}
}

func TestLeaseElectorRetainsValidLeadershipAcrossTransientRenewalError(t *testing.T) {
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "demo", "sdo", "first", 10*time.Second)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire leadership: leader=%v err=%v", leader, err)
	}
	guarded, cancel, err := elector.GuardContext(context.Background())
	if err != nil {
		t.Fatalf("guard context: %v", err)
	}
	defer cancel()
	failed := false
	client.PrependReactor("get", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failed {
			return false, nil, nil
		}
		failed = true
		return true, nil, errors.New("transient API timeout")
	})

	if leader, err := elector.TryAcquireOrRenew(context.Background()); err == nil || leader {
		t.Fatalf("transient renewal unexpectedly succeeded: leader=%v err=%v", leader, err)
	}
	if !elector.IsLeader() {
		t.Fatal("transient renewal error deactivated a still-valid Lease")
	}
	select {
	case <-guarded.Done():
		t.Fatal("transient renewal error cancelled guarded work before Lease expiry")
	default:
	}
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("retry did not renew leadership: leader=%v err=%v", leader, err)
	}
}

func TestLeadershipRenewalContinuesWhileControllerLoopIsBusy(t *testing.T) {
	client := fake.NewSimpleClientset()
	var updates atomic.Int32
	client.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		updates.Add(1)
		return false, nil, nil
	})
	elector := NewLeaseElector(client, "demo", "sdo", "first", time.Second)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire leadership: leader=%v err=%v", leader, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errors := maintainLeadership(ctx, elector, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for updates.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if updates.Load() < 2 {
		select {
		case err := <-errors:
			t.Fatalf("leadership renewal stopped: %v", err)
		default:
			t.Fatal("leadership did not renew independently of the controller loop")
		}
	}
	lease, err := client.CoordinationV1().Leases("demo").Get(context.Background(), "sdo", metav1.GetOptions{})
	if err != nil || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "first" {
		t.Fatalf("renewed Lease is invalid: lease=%#v err=%v", lease, err)
	}
}

func TestLeaseElectorExpiredLeaseHandoff(t *testing.T) {
	client := fake.NewSimpleClientset()
	first := NewLeaseElector(client, "demo", "sdo", "first", 10*time.Second)
	second := NewLeaseElector(client, "demo", "sdo", "second", 10*time.Second)
	now := time.Unix(100, 0)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	if leader, err := first.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("first failed to acquire: leader=%v err=%v", leader, err)
	}
	now = now.Add(11 * time.Second)
	if leader, err := second.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("successor failed to take expired Lease: leader=%v err=%v", leader, err)
	}
}

func TestLeaseElectorExpiresAndCancelsGuardedActionsWhileProcessIsPaused(t *testing.T) {
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "demo", "sdo", "first", 10*time.Second)
	now := time.Now().UTC()
	elector.now = func() time.Time { return now }
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire leadership: leader=%v err=%v", leader, err)
	}
	guarded, cancel, err := elector.GuardContext(context.Background())
	if err != nil {
		t.Fatalf("guard context: %v", err)
	}
	defer cancel()

	now = now.Add(11 * time.Second)
	if elector.IsLeader() {
		t.Fatal("paused elector retained leadership past Lease expiry")
	}
	select {
	case <-guarded.Done():
	case <-time.After(time.Second):
		t.Fatal("expired leadership did not cancel guarded action")
	}
}

func TestLeaseElectorStaleExpiryCannotDeactivateRenewedGeneration(t *testing.T) {
	elector := NewLeaseElector(fake.NewSimpleClientset(), "demo", "sdo", "first", 10*time.Second)
	now := time.Now().UTC()
	elector.now = func() time.Time { return now }
	elector.activate(now.Add(10 * time.Second))
	staleGeneration := elector.generation.Load()

	now = now.Add(time.Second)
	elector.activate(now.Add(10 * time.Second))
	elector.expire(staleGeneration)

	if !elector.IsLeader() {
		t.Fatal("stale expiry callback deactivated a renewed Lease generation")
	}
}
