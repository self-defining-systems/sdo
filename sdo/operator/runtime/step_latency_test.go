package runtime

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/flowcontrol"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

// clearDetector records an evaluation without findings, like a health
// detector that watches Pods during a restart storm.
type clearDetector struct{ spec sdk.DetectorSpec }

func (d clearDetector) Spec() sdk.DetectorSpec { return d.spec }

func (clearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// absenceProbe reports the first evaluation that observes the ConfigMap gone.
type absenceProbe struct {
	configMapPresenceDetector
	once     *sync.Once
	observed chan time.Time
}

func (d absenceProbe) Detect(ctx context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.configMapPresenceDetector.Detect(ctx, snapshot)
	if len(findings) > 0 {
		d.once.Do(func() { d.observed <- time.Now() })
	}
	return findings, err
}

type stormResult struct {
	detection time.Duration
	steps     int64
	apiCalls  int64
	// maxStep is the longest single loop iteration (StepEvents plus
	// PersistState), which bounds how long a new watch event can wait.
	maxStep time.Duration
}

// runRestartStorm drives the production loop shape (watch notification,
// StepEvents, PersistState) against a fake API server whose non-watch requests
// pass through a client-go token bucket, then measures how long a ConfigMap
// delete that follows a Pod event storm waits for its watching detector.
func runRestartStorm(t *testing.T, qps float32, burst int, podEvents int, spacing time.Duration) stormResult {
	t.Helper()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"},
	})
	limiter := flowcontrol.NewTokenBucketRateLimiter(qps, burst)
	var apiCalls atomic.Int64
	client.PrependReactor("*", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		switch action.GetVerb() {
		case "get", "create", "update", "patch", "delete":
			limiter.Accept()
			apiCalls.Add(1)
		}
		return false, nil, nil
	})
	probe := absenceProbe{
		configMapPresenceDetector: configMapPresenceDetector{spec: incidentSpec(configMapWatch), configMap: "mongo-geo-script"},
		once:                      &sync.Once{}, observed: make(chan time.Time, 1),
	}
	podHealth := healthSpec(podWatch)
	podHealth.Batching.Debounce = 0
	detectors := []sdk.Detector{clearDetector{spec: podHealth}, probe}
	informerCache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: "demo", Client: client, StateConfigMapName: "sdo-controller-state",
	}, detectors)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informerCache.Start(ctx)
	if err := informerCache.WaitForSync(ctx); err != nil {
		t.Fatalf("wait for sync: %v", err)
	}
	drainWatchEvents(informerCache)
	controller, err := NewController(
		testControllerConfig(), detectors, informerCache,
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.AttachStateStore(ctx, NewConfigMapStateStore(client, "demo", "sdo-controller-state")); err != nil {
		t.Fatalf("attach state store: %v", err)
	}
	if err := controller.Step(ctx, time.Now().UTC(), nil); err != nil {
		t.Fatalf("baseline step: %v", err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("baseline persist: %v", err)
	}

	var steps atomic.Int64
	var maxStep time.Duration // written by the loop, read after loopDone
	loopErrors := make(chan error, 1)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-informerCache.Notifications():
				stepStart := time.Now()
				if err := controller.StepEvents(ctx, time.Now().UTC(), informerCache.TakeEvents()); err != nil {
					loopErrors <- err
					return
				}
				if err := controller.PersistState(ctx); err != nil {
					if ctx.Err() == nil {
						loopErrors <- err
					}
					return
				}
				steps.Add(1)
				maxStep = max(maxStep, time.Since(stepStart))
			}
		}
	}()

	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	configMaps := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	for index := 0; index < podEvents; index++ {
		// The tracker feeds the informers' watches directly, so the storm's
		// writers (kubelet, ReplicaSet controller) do not share the
		// controller's client-side rate limiter.
		if err := client.Tracker().Create(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", index), Namespace: "demo"},
		}, "demo"); err != nil {
			t.Fatalf("create pod: %v", err)
		}
		time.Sleep(spacing)
	}
	deletedAt := time.Now()
	if err := client.Tracker().Delete(configMaps, "demo", "mongo-geo-script"); err != nil {
		t.Fatalf("delete ConfigMap: %v", err)
	}
	var detection time.Duration
	select {
	case observedAt := <-probe.observed:
		detection = observedAt.Sub(deletedAt)
	case err := <-loopErrors:
		t.Fatalf("controller loop: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("ConfigMap watcher was never evaluated after the delete")
	}
	cancel()
	<-loopDone
	return stormResult{detection: detection, steps: steps.Load(), apiCalls: apiCalls.Load(), maxStep: maxStep}
}

// Reproduces the restart-storm finding: with client-go's former QPS 5 and a
// Get plus Update per step, steps were spaced 400 ms apart and a ConfigMap
// delete waited behind them.
func TestRestartStormEvaluatesConfigMapWatcherPromptly(t *testing.T) {
	result := runRestartStorm(
		t, core.DefaultKubernetesClientQPS, core.DefaultKubernetesClientBurst, 300, 5*time.Millisecond,
	)
	if result.detection > 100*time.Millisecond {
		t.Fatalf("ConfigMap watcher evaluated %s after the delete, want at most 100ms", result.detection)
	}
	// Baseline Load and create, then at most one state write per loop step.
	if result.apiCalls > result.steps+3 {
		t.Fatalf("state persistence made %d rate-limited API calls over %d steps", result.apiCalls, result.steps)
	}
}

// TestRestartStormStepLatencyReport compares client rate limits. Run it with
// SDO_STEP_LATENCY_REPORT=1 go test -run TestRestartStormStepLatencyReport -v.
func TestRestartStormStepLatencyReport(t *testing.T) {
	if os.Getenv("SDO_STEP_LATENCY_REPORT") == "" {
		t.Skip("set SDO_STEP_LATENCY_REPORT=1 to run the timed restart-storm comparison")
	}
	for _, scenario := range []struct {
		qps       float32
		burst     int
		podEvents int
		spacing   time.Duration
	}{
		{5, 10, 50, 20 * time.Millisecond},
		{50, 100, 50, 20 * time.Millisecond},
		{5, 10, 300, 5 * time.Millisecond},
		{50, 100, 300, 5 * time.Millisecond},
	} {
		result := runRestartStorm(t, scenario.qps, scenario.burst, scenario.podEvents, scenario.spacing)
		t.Logf(
			"qps=%v burst=%d pods=%d every %s: ConfigMap delete evaluated after %s; %d steps, longest %s, %d rate-limited API calls",
			scenario.qps, scenario.burst, scenario.podEvents, scenario.spacing,
			result.detection.Round(100*time.Microsecond), result.steps, result.maxStep.Round(100*time.Microsecond),
			result.apiCalls,
		)
	}
}
