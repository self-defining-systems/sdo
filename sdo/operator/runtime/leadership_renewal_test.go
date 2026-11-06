package runtime

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	kclock "k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

// client-go's limiter refuses a wait that overruns the caller's deadline with
// an error that wraps no context error.
func rateLimiterWaitError() error {
	return fmt.Errorf("get leader Lease: client rate limiter Wait returned an error: %s", errors.New("rate: Wait(n=1) would exceed context deadline"))
}

// asyncExpiryClock runs AfterFunc callbacks on their own goroutine, as real
// timers do: the fake clock runs them inline under its own lock, which
// deadlocks against the elector re-arming its expiry timer.
type asyncExpiryClock struct {
	*clocktesting.FakeClock
	ticker chan struct{}
}

func newAsyncExpiryClock() asyncExpiryClock {
	return asyncExpiryClock{clocktesting.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), make(chan struct{}, 1)}
}

// NewTicker tells the test the renewal loop is waiting, so no step is lost
// before its first tick exists.
func (c asyncExpiryClock) NewTicker(d time.Duration) kclock.Ticker {
	ticker := c.FakeClock.NewTicker(d)
	c.ticker <- struct{}{}
	return ticker
}

func (c asyncExpiryClock) AfterFunc(d time.Duration, f func()) kclock.Timer {
	return c.FakeClock.AfterFunc(d, func() { go f() })
}

func TestClientSideThrottlingIsTransient(t *testing.T) {
	if !transientAPIError(rateLimiterWaitError()) {
		t.Fatal("a rate limiter refusal is local throttling, not a verdict a retry cannot fix")
	}
}

// Renewal that fails under client-side throttling after the Lease lapsed
// locally keeps retrying (the guard refuses effects meanwhile); it must not end
// the process, which is what turned a saturated node into a controller restart.
func TestRenewalUnderThrottlingNeverEndsTheProcess(t *testing.T) {
	client := fake.NewSimpleClientset()
	clock := newAsyncExpiryClock()
	elector := NewLeaseElectorWithClock(client, "ns", "lease", "me", 60*time.Second, clock)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire: %v %v", leader, err)
	}
	attempts := make(chan struct{}, 64)
	client.PrependReactor("*", "leases", func(clienttesting.Action) (bool, kruntime.Object, error) {
		attempts <- struct{}{}
		return true, nil, rateLimiterWaitError()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := maintainLeadership(ctx, elector, 5*time.Second, clock)
	<-clock.ticker
	// Step the clock until twenty renewals were attempted. The renewal loop
	// handles each attempt's result before it waits for the next tick, so by
	// the twentieth the earlier ones, past the Lease's expiry, have been handled.
	for seen := 0; seen < 20; {
		clock.Step(5 * time.Second)
		select {
		case <-attempts:
			seen++
		default:
			runtime.Gosched()
		}
	}
	if elector.IsLeader() {
		t.Fatal("the Lease should have lapsed locally during the outage")
	}
	select {
	case err := <-errs:
		t.Fatalf("throttled renewal ended the process: %v", err)
	default:
	}
}

// A Lease another holder took is a typed stop, not an anonymous error.
func TestTakenLeaseIsTheTypedLeadershipLoss(t *testing.T) {
	client := fake.NewSimpleClientset()
	clock := newAsyncExpiryClock()
	elector := NewLeaseElectorWithClock(client, "ns", "lease", "me", 60*time.Second, clock)
	if leader, err := elector.TryAcquireOrRenew(context.Background()); err != nil || !leader {
		t.Fatalf("acquire: %v %v", leader, err)
	}
	other := "someone-else"
	lease, err := client.CoordinationV1().Leases("ns").Get(context.Background(), "lease", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lease.Spec.HolderIdentity = &other
	renew := clock.Now().Add(24 * time.Hour) // never expires while the test steps the clock
	lease.Spec.RenewTime = &metav1.MicroTime{Time: renew}
	if _, err := client.CoordinationV1().Leases("ns").Update(context.Background(), lease, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := maintainLeadership(ctx, elector, 5*time.Second, clock)
	<-clock.ticker
	for spins := 0; ; spins++ {
		if spins > 1_000_000 {
			t.Fatal("no stop signal")
		}
		select {
		case err := <-errs:
			if !errors.Is(err, ErrLeadershipLost) {
				t.Fatalf("want ErrLeadershipLost, got %v", err)
			}
			return
		default:
			clock.Step(5 * time.Second)
			runtime.Gosched()
		}
	}
}
