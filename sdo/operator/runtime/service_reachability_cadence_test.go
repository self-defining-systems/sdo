package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

// recordingDialer answers every dial from a fake clock and records it per
// target; failing targets answer with a refusal.
type recordingDialer struct {
	clock *clocktesting.FakeClock
	mu    sync.Mutex
	fail  map[string]bool
	dials map[string][]time.Time
}

func (d *recordingDialer) DialTargets(_ context.Context, request prober.DialRequest) (prober.DialResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var response prober.DialResponse
	for _, target := range request.Targets {
		now := d.clock.Now()
		d.dials[target.ID()] = append(d.dials[target.ID()], now)
		sample := servicehealth.DialSample{At: now, OK: !d.fail[target.ID()]}
		if !sample.OK {
			sample.Class = servicehealth.DialRefused
		}
		response.Results = append(response.Results, prober.DialResult{Target: target, Sample: sample})
	}
	return response, nil
}

func (d *recordingDialer) setFailing(id string, failing bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail[id] = failing
}

func (d *recordingDialer) since(id string, from time.Time) []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []time.Time
	for _, at := range d.dials[id] {
		if !at.Before(from) {
			out = append(out, at)
		}
	}
	return out
}

type staticSnapshots struct{ snapshot sdktest.Snapshot }

func (s staticSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return s.snapshot, nil
}

func cadenceObserver(t *testing.T) (*ServiceReachabilityObserver, *recordingDialer, *clocktesting.FakeClock, time.Time) {
	t.Helper()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(start)
	// Both Services have been ready long before the window, so no catch-up applies.
	ready := metav1.NewTime(start.Add(-time.Hour))
	pod := func(name, app string) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"app": app}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: ready}}}}
	}
	service := func(name, ip string, port int32) corev1.Service {
		return corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
			Spec: corev1.ServiceSpec{ClusterIP: ip, Selector: map[string]string{"app": name}, Ports: []corev1.ServicePort{{Port: port}}}}
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app",
		ServiceList: []corev1.Service{service("ssh", "10.0.0.1", 22), service("web", "10.0.0.2", 80)},
		PodList:     []corev1.Pod{pod("ssh-0", "ssh"), pod("web-0", "web")}}
	dialer := &recordingDialer{clock: clock, fail: map[string]bool{}, dials: map[string][]time.Time{}}
	observer := NewServiceReachabilityObserver(ServiceReachabilityConfig{Clock: clock, Prober: dialer, Snapshots: staticSnapshots{snapshot}})
	return observer, dialer, clock, start
}

// tick runs the round the Start loop runs on each interval tick.
func tick(observer *ServiceReachabilityObserver, clock *clocktesting.FakeClock, seconds int) {
	for i := 0; i < seconds; i++ {
		clock.Step(time.Second)
		observer.Cycle(context.Background())
	}
}

func TestHealthyTargetDialRateIsBoundedByIdleInterval(t *testing.T) {
	observer, dialer, clock, start := cadenceObserver(t)
	observer.Cycle(context.Background())
	tick(observer, clock, 600)
	warm := start.Add(time.Duration(servicehealth.DefaultReachabilityFailures) * time.Second)
	for _, id := range []string{"ssh:22", "web:80"} {
		all := dialer.since(id, start)
		warmup := 0
		for _, at := range all {
			if at.Before(warm) {
				warmup++
			}
		}
		// Warm-up: Failures dials, one interval apart, make the target known.
		if warmup != servicehealth.DefaultReachabilityFailures {
			t.Fatalf("%s warm-up dials = %d, want %d", id, warmup, servicehealth.DefaultReachabilityFailures)
		}
		// Steady state: at most one dial per idle interval over the window.
		steady := len(all) - warmup
		bound := int((clock.Now().Sub(warm))/servicehealth.DefaultReachabilityIdleInterval) + 1
		if steady > bound {
			t.Fatalf("%s healthy dials = %d over %s, want at most %d", id, steady, clock.Now().Sub(warm), bound)
		}
	}
	window, _ := observer.Window()
	if window.IdleInterval != servicehealth.DefaultReachabilityIdleInterval {
		t.Fatalf("window idle interval = %s", window.IdleInterval)
	}
}

func TestFailingTargetIsReprobedEveryInterval(t *testing.T) {
	observer, dialer, clock, _ := cadenceObserver(t)
	observer.Cycle(context.Background())
	tick(observer, clock, 60)
	dialer.setFailing("ssh:22", true)
	failedAt := clock.Now()
	tick(observer, clock, 30)
	after := dialer.since("ssh:22", failedAt)
	// The first failing dial is at most one idle interval away; afterwards every second.
	if lag := after[0].Sub(failedAt); lag > servicehealth.DefaultReachabilityIdleInterval {
		t.Fatalf("first failing dial lag = %s", lag)
	}
	for i := 2; i < len(after); i++ {
		if gap := after[i].Sub(after[i-1]); gap != time.Second {
			t.Fatalf("failing re-probe gap = %s, want 1s", gap)
		}
	}
	if len(after) < 20 {
		t.Fatalf("failing target dialed %d times in 30s", len(after))
	}
	// The healthy neighbor stays idle.
	if got := len(dialer.since("web:80", failedAt)); got > 4 {
		t.Fatalf("healthy neighbor dialed %d times in 30s", got)
	}
	// Recovery: dialed fast until Failures successes in a row, then idle again.
	dialer.setFailing("ssh:22", false)
	recovered := clock.Now()
	tick(observer, clock, 60)
	if got := len(dialer.since("ssh:22", recovered)); got > servicehealth.DefaultReachabilityFailures+6 {
		t.Fatalf("recovered target dialed %d times in 60s", got)
	}
}

func TestStallProducesNoDialBurst(t *testing.T) {
	observer, dialer, clock, _ := cadenceObserver(t)
	observer.Cycle(context.Background())
	tick(observer, clock, 30)
	// The loop stalls for 8s. On resume the runtime delivers the stalled tick,
	// the one queued behind it, and a wake, back to back.
	clock.Step(8 * time.Second)
	resumed := clock.Now()
	observer.Cycle(context.Background())
	observer.Cycle(context.Background())
	observer.CatchUp(context.Background())
	observer.Cycle(context.Background())
	if got := len(dialer.since("ssh:22", resumed)); got > 1 {
		t.Fatalf("a stall produced %d dials of a healthy target, want at most 1", got)
	}
	// A failing target gets one dial on resume, not one per queued round.
	dialer.setFailing("web:80", true)
	tick(observer, clock, 12)
	clock.Step(8 * time.Second)
	resumed = clock.Now()
	observer.Cycle(context.Background())
	observer.Cycle(context.Background())
	observer.CatchUp(context.Background())
	if got := len(dialer.since("web:80", resumed)); got != 1 {
		t.Fatalf("a stall produced %d dials of a failing target, want 1", got)
	}
}
