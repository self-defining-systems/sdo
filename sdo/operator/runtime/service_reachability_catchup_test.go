package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

// fakeClock is the observer's injected clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

// clockDialer answers every dial at the clock's time, refusing while refuse is set.
type clockDialer struct {
	now    func() time.Time
	refuse atomic.Bool
	dialed atomic.Int32
}

func (d *clockDialer) DialTargets(_ context.Context, request prober.DialRequest) (prober.DialResponse, error) {
	response := prober.DialResponse{}
	for _, target := range request.Targets {
		d.dialed.Add(1)
		sample := servicehealth.DialSample{At: d.now(), Duration: time.Millisecond, OK: !d.refuse.Load()}
		if !sample.OK {
			sample.Class, sample.Error = servicehealth.DialRefused, "connection refused"
		}
		response.Results = append(response.Results, prober.DialResult{Target: target, Sample: sample})
	}
	return response, nil
}

func readyGeoSince(at time.Time) *mutableSnapshots {
	snapshot := reachabilityApp(8083)
	snapshot.PodList[0].Status.Conditions[0].LastTransitionTime = metav1.NewTime(at)
	return &mutableSnapshots{snapshot: snapshot}
}

func reachabilityConfirmed(t *testing.T, observer *ServiceReachabilityObserver, snapshots SnapshotProvider) bool {
	t.Helper()
	snapshot, err := TrafficSnapshotProvider{Base: snapshots, Reachability: observer}.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	detector := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", Interval: 10 * time.Second})
	findings, err := detector.Detect(context.Background(), snapshot)
	if err != nil || len(findings) != 0 {
		t.Fatalf("a reachable Service must not fire: %v %v", findings, err)
	}
	return detector.(interface{ Confirmed(sdk.DetectionContext) bool }).Confirmed(snapshot)
}

// TestReachabilityObserverFillsARestartedWindowAtTheCatchUpSpacing: a repair
// that replaces the only ready pod restarts the clearing window. The same
// three dials after the new Ready transition fill it CatchUpSpacing apart,
// not one full interval apart, and each catch-up round wakes the detectors so
// the window clears in the evaluation of the round that fills it.
func TestReachabilityObserverFillsARestartedWindowAtTheCatchUpSpacing(t *testing.T) {
	start := time.Date(2026, 10, 26, 15, 24, 0, 0, time.UTC)
	clock := &fakeClock{now: start}
	dialer := &clockDialer{now: clock.Now}
	snapshots := readyGeoSince(start.Add(-time.Hour))
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: dialer, Snapshots: snapshots, Interval: 10 * time.Second, Timeout: time.Second, Now: clock.Now,
	})
	observer.Cycle(context.Background())

	ready := start.Add(5 * time.Second)
	snapshots.set(readyGeoSince(ready).snapshot)
	clock.set(ready)
	if reachabilityConfirmed(t, observer, snapshots) {
		t.Fatal("dials before the new Ready transition must not confirm the window")
	}
	for step := 1; step <= servicehealth.DefaultReachabilityFailures; step++ {
		clock.set(ready.Add(time.Duration(step) * reachabilityCatchUpSpacing))
		dialed, pending := observer.CatchUp(context.Background())
		if !dialed {
			t.Fatalf("catch-up round %d dialed nothing", step)
		}
		last := step == servicehealth.DefaultReachabilityFailures
		if pending == last || reachabilityConfirmed(t, observer, snapshots) != last {
			t.Fatalf("round %d: pending=%v, want the window to fill exactly at round %d", step, pending, servicehealth.DefaultReachabilityFailures)
		}
	}
	if dialed, pending := observer.CatchUp(context.Background()); dialed || pending {
		t.Fatal("a filled window needs no more catch-up dials")
	}
}

// TestReachabilityObserverCatchUpKeepsTheFailureSpacing: a failed catch-up dial
// ends the catch-up and holds the target out of full rounds for one interval,
// so three consecutive failures still span two intervals.
func TestReachabilityObserverCatchUpKeepsTheFailureSpacing(t *testing.T) {
	ready := time.Date(2026, 10, 26, 15, 24, 0, 0, time.UTC)
	clock := &fakeClock{now: ready.Add(time.Second)}
	dialer := &clockDialer{now: clock.Now}
	dialer.refuse.Store(true)
	snapshots := readyGeoSince(ready)
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: dialer, Snapshots: snapshots, Interval: 10 * time.Second, Timeout: time.Second, Now: clock.Now,
	})
	if dialed, pending := observer.CatchUp(context.Background()); !dialed || pending {
		t.Fatalf("a failed catch-up dial must end the catch-up: dialed=%v pending=%v", dialed, pending)
	}
	clock.set(ready.Add(3 * time.Second))
	observer.Cycle(context.Background())
	if got := dialer.dialed.Load(); got != 1 {
		t.Fatalf("a full round within one interval of a failed catch-up dial must skip the target, dialed %d", got)
	}
	for _, at := range []time.Duration{11, 21} {
		clock.set(ready.Add(at * time.Second))
		observer.Cycle(context.Background())
	}
	window, _ := observer.Window()
	samples := window.Targets[0].Samples
	if len(samples) != 3 || samples[2].At.Sub(samples[0].At) < 2*10*time.Second {
		t.Fatalf("failure run must span two intervals: %+v", samples)
	}
}

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

// TestReachabilityObserverBoundsCatchUpPerInterval: a Ready time no dial ever
// passes (kubelet clock ahead of the prober) costs at most Failures extra
// dials between full rounds or Pod changes.
func TestReachabilityObserverBoundsCatchUpPerInterval(t *testing.T) {
	now := time.Date(2026, 10, 26, 15, 24, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	dialer := &clockDialer{now: clock.Now}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{
		Prober: dialer, Snapshots: readyGeoSince(now.Add(time.Minute)), Interval: 10 * time.Second, Now: clock.Now,
	})
	for range 10 {
		observer.CatchUp(context.Background())
	}
	if got := dialer.dialed.Load(); got != servicehealth.DefaultReachabilityFailures {
		t.Fatalf("catch-up dials between full rounds: %d", got)
	}
	observer.Cycle(context.Background())
	if dialed, _ := observer.CatchUp(context.Background()); !dialed {
		t.Fatal("a full round renews the catch-up budget")
	}
}
