//go:build integration

package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"


)

// TestReachabilityObserverWakesOnReadinessBetweenRounds runs the observer
// loop with an interval far longer than the test: after a Pod change wakes
// it, the restarted window fills at the catch-up spacing.
func TestReachabilityObserverWakesOnReadinessBetweenRounds(t *testing.T) {
	dialer := &clockDialer{now: time.Now}
	snapshots := readyGeoSince(time.Now().Add(-time.Hour))
	var notified atomic.Int32
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: dialer, Snapshots: snapshots, Interval: time.Hour, Timeout: time.Second,
		CatchUpSpacing: 5 * time.Millisecond, Notify: func() { notified.Add(1) },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer.Start(ctx)
	defer observer.Stop()
	waitConfirmed := func(what string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !reachabilityConfirmed(t, observer, snapshots) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: window not filled within 5s at a 1h interval", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitConfirmed("startup")
	before := notified.Load()
	// Ready transitions have second precision: become Ready on the next
	// second, after every startup dial.
	ready := time.Now().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(ready))
	snapshots.set(readyGeoSince(ready).snapshot)
	if reachabilityConfirmed(t, observer, snapshots) {
		t.Fatal("startup dials must not confirm the window a replaced pod restarted")
	}
	observer.Wake()
	waitConfirmed("after a replaced pod became Ready")
	if notified.Load() <= before {
		t.Fatal("catch-up rounds must wake the detectors")
	}
}

