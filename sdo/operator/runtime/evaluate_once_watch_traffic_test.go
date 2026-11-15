//go:build integration

package runtime

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// liveProber reports a fresh window at every read: user requests succeed until
// the application is broken, then every request is refused. It stands for a
// prober that keeps sampling while the evaluation runs.
type liveProber struct {
	broken   atomic.Bool
	workload traffic.Workload
}

func (p *liveProber) Windows(context.Context) (map[string]traffic.Window, error) {
	now := time.Now().UTC()
	outcome, errText := traffic.OutcomeOK, ""
	if p.broken.Load() {
		outcome, errText = traffic.OutcomeError, "dial tcp: connection refused"
	}
	window := traffic.Window{Workload: p.workload, ObservedAt: now}
	for _, scenario := range p.workload.Scenarios {
		observed := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: scenario.ID, Target: traffic.Target{Service: "web", Port: 80}}, Qualified: true}
		for i := 5; i >= 1; i-- {
			observed.Samples = append(observed.Samples, traffic.Sample{At: now.Add(-time.Duration(i) * 100 * time.Millisecond), Outcome: outcome, Error: errText, DialFailed: errText != ""})
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	return map[string]traffic.Window{p.workload.Name: window}, nil
}
func (*liveProber) Burst(context.Context, prober.BurstRequest) (prober.BurstResult, error) {
	return prober.BurstResult{}, nil
}
func (*liveProber) Reset(context.Context) error { return nil }

// A component that users' operations depend on goes down while an evaluation
// watches the application: the user-facing detector must name the failing
// scenario within the watch. The watch reads the prober for as long as it
// runs, not only during the warm-up.
func TestEvaluateOnceWatchKeepsReadingTheProberSoABreakRaisesTheUserTest(t *testing.T) {
	workload := pacedWorkload(3, 5)
	workload.Scenarios = []traffic.WorkloadScenario{{ID: "call-projects", Weight: 1}}
	api := &liveProber{workload: workload}
	detector := traffic.NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1, MinDuration: time.Millisecond}, Playbooks: []string{"health-objective"}, OriginatingCommit: "synthetic"}, workload.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	time.AfterFunc(1500*time.Millisecond, func() { api.broken.Store(true) })
	var stdout, stderr bytes.Buffer
	err := RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "shop", "--watch", "200ms"},
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "shop"}}}}},
		Client:   fake.NewSimpleClientset(),
		Traffic:  api,
		Stdout:   &stdout, Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("evaluate-once: %v (stderr=%s)", err, stderr.String())
	}
	var healthy, named bool
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		verdict, err := evaluation.Decode([]byte(line))
		if err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if verdict.Status == evaluation.Clear {
			healthy = true
		}
		for _, finding := range verdict.Findings {
			if strings.Contains(finding.Summary+finding.RuleID, "call-projects") {
				named = true
			}
		}
	}
	if !healthy {
		t.Fatalf("the watch must start clear: %s", stdout.String())
	}
	if !named {
		t.Fatalf("no verdict named the failing user test call-projects after the application broke: %s", stdout.String())
	}
}
