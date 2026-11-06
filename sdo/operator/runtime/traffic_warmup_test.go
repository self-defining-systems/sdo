package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// pacedProber is a prober that started when the test began and completes one
// sample per scenario every interval of the fake clock, as a cold prober does.
// Every read takes one poll period of fake time, so the waiter under test and
// the clock advance in lockstep.
type pacedProber struct {
	clock    *clocktesting.FakeClock
	started  time.Time
	workload traffic.Workload
	interval time.Duration
}

func (p *pacedProber) Windows(context.Context) (map[string]traffic.Window, error) {
	p.clock.Step(100 * time.Millisecond)
	now := p.clock.Now()
	count := int(now.Sub(p.started) / p.interval)
	window := traffic.Window{Workload: p.workload, ObservedAt: now}
	for _, scenario := range p.workload.Scenarios {
		observed := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: scenario.ID}, Qualified: count > 0}
		for i := count; i >= 1; i-- {
			observed.Samples = append(observed.Samples, traffic.Sample{At: now.Add(-time.Duration(i-1) * p.interval), Outcome: traffic.OutcomeOK})
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	return map[string]traffic.Window{p.workload.Name: window}, nil
}
func (*pacedProber) Burst(context.Context, prober.BurstRequest) (prober.BurstResult, error) {
	return prober.BurstResult{}, nil
}
func (*pacedProber) Reset(context.Context) error { return nil }

func pacedWorkload(minSamples, window int) traffic.Workload {
	slo := traffic.SLO{MinSamples: minSamples, Window: window}
	return traffic.Workload{
		Name: "probe-call", RatePerSecond: 2, SLO: slo,
		Scenarios: []traffic.WorkloadScenario{{ID: "home", Weight: 1}, {ID: "journey", Weight: 1}},
	}
}

// The warm condition is what the accepted workload declares, not a constant:
// a workload whose SLO needs N samples per scenario is warm after N, not 1.
func TestWaitWarmWaitsForTheWorkloadsOwnMinimumSamples(t *testing.T) {
	for _, tc := range []struct{ minSamples, window int }{{1, 5}, {3, 5}, {4, 4}, {8, 10}, {20, 20}} {
		t.Run(fmt.Sprintf("min%d_window%d", tc.minSamples, tc.window), func(t *testing.T) {
			clock := clocktesting.NewFakeClock(time.Unix(1000, 0).UTC())
			workload := pacedWorkload(tc.minSamples, tc.window)
			// Two scenarios at 2/s: each completes a sample per second.
			api := &pacedProber{clock: clock, started: clock.Now(), workload: workload, interval: time.Second}
			observer := NewTrafficObserver(api, []string{workload.Name}, nil, 0)
			observer.Clock = clock
			started := clock.Now()
			budget := time.Duration(tc.minSamples+10) * time.Second
			if !observer.WaitWarm(context.Background(), budget) {
				t.Fatalf("not warm within %s, want warm once each scenario holds %d samples", budget, tc.minSamples)
			}
			if got, want := clock.Since(started), time.Duration(tc.minSamples)*time.Second; got < want {
				t.Fatalf("declared warm after %s, before %d samples per scenario (%s) existed", got, tc.minSamples, want)
			}
		})
	}
}

// A one-shot evaluation must wait for the same condition, so a healthy
// application is judged clear instead of pending on a cold prober.
func TestEvaluateOnceWaitsForTheWorkloadsOwnMinimumSamples(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Unix(1000, 0).UTC())
	workload := pacedWorkload(3, 5)
	api := &pacedProber{clock: clock, started: clock.Now(), workload: workload, interval: time.Second}
	detector := traffic.NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}, Interval: 15 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Playbooks: []string{"health-objective"}, OriginatingCommit: "synthetic"}, workload.Name)
	var stdout, stderr bytes.Buffer
	err := RunWithOptions(context.Background(), []sdk.Detector{detector}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "shop"},
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "shop"}}}}},
		Client:   fake.NewSimpleClientset(),
		Traffic:  api,
		Clock:    clock,
		Stdout:   &stdout, Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("evaluate-once: %v (stderr=%s)", err, stderr.String())
	}
	var result evaluationResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode %q: %v", stdout.String(), err)
	}
	if result.Status != "clear" {
		t.Fatalf("status = %q, want clear after the prober gathered %d samples per scenario: %s", result.Status, 3, stdout.String())
	}
}
