//go:build integration

package runtime

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/install"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// syncBuffer is a bytes.Buffer safe for the controller's background writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// coldProber never qualifies a scenario, as a prober whose probe user is not
// provisioned yet.
type coldProber struct{ workload traffic.Workload }

// Start-up does not wait for probes to qualify: a controller whose prober has
// not produced one sample serves, and acknowledges a reset request, while the
// warm-up (here bounded by hours) is still running.
func TestStartupServesResetsWhileTheTrafficWarmupRuns(t *testing.T) {
	workload := pacedWorkload(1000, 1000)
	workload.RatePerSecond = 0.001
	api := &coldProber{workload: workload}
	detector := traffic.NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}, Interval: 15 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Playbooks: []string{"health-objective"}, OriginatingCommit: "synthetic"}, workload.Name)
	client := fake.NewSimpleClientset()
	resets := install.ResetRequests{Client: client, Namespace: "sdo"}
	if err := resets.Request(context.Background(), "g1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
			Args:     []string{"--namespace", "shop", "--control-namespace", "sdo", "--app-root", t.TempDir(), "--application", "shop", "--source-commit", harnessAcceptedCommitForTest, "--repository-mount-path", t.TempDir(), "--toolchain-image", "example.invalid/toolchain"},
			Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "web-0", Namespace: "shop"}}}}},
			Client:   client,
			Traffic:  api, Dispatcher: &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, AcceptedScope: harnessScopeForTest,
			Stdout: &stdout, Stderr: &stderr,
		})
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if acknowledged, _ := resets.Acknowledged(context.Background()); acknowledged == "g1" {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("controller ended before serving: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("reset not acknowledged while the warm-up ran\nstdout=%s\nstderr=%s", stdout.String(), stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	out := stdout.String()
	for _, want := range []string{`"startup_stage":"informer_sync"`, `"startup_stage":"first_evaluation"`, `"controller_serving":true`, `"synthetic_traffic":"warming"`} {
		if !strings.Contains(out, want) {
			t.Errorf("start-up log lacks %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"synthetic_traffic_warmup_ms"`) {
		t.Errorf("the warm-up was reported concluded although no scenario qualified:\n%s", out)
	}
}

const harnessAcceptedCommitForTest = "0123456789abcdef0123456789abcdef01234567"

func harnessScopeForTest(context.Context, string) (sdk.ObservationScope, string, error) {
	scope, err := observation.Parse([]byte("---\nschema_version: 1\n---\nSynthetic test architecture.\n"))
	return scope, harnessAcceptedCommitForTest, err
}

func (*coldProber) DialTargets(context.Context, prober.DialRequest) (prober.DialResponse, error) {
	return prober.DialResponse{}, nil
}

func (p *coldProber) Windows(context.Context) (map[string]traffic.Window, error) {
	window := traffic.Window{Workload: p.workload, ObservedAt: time.Now()}
	for _, scenario := range p.workload.Scenarios {
		window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: scenario.ID}})
	}
	return map[string]traffic.Window{p.workload.Name: window}, nil
}

func (*coldProber) Burst(context.Context, prober.BurstRequest) (prober.BurstResult, error) {
	return prober.BurstResult{}, nil
}

func (*coldProber) Reset(context.Context) error { return nil }
