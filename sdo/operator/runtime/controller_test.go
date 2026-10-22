package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type changingProvider struct{ snapshot sdk.DetectionContext }

func (p *changingProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return p.snapshot, nil
}

func TestControllerCapturesOpenAndVerifiedSnapshots(t *testing.T) {
	root := t.TempDir()
	config := testControllerConfig()
	config.Namespace = "demo"
	config.CorpusSessionRoot = root
	provider := &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "demo"}}}}}
	detector := controllerDetector("health", time.Second, stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := controller.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	open, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
	if err != nil || !strings.Contains(string(open), "broken") {
		t.Fatalf("open: %s, %v", open, err)
	}
	provider.snapshot = sdktest.Snapshot{NamespaceName: "demo", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "fixed", Namespace: "demo"}}}}
	awaitDispatchCompletionQueued(t, controller)
	for i := 2; i < 4; i++ {
		if err := controller.Step(context.Background(), time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	fixed, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
	if err != nil || !strings.Contains(string(fixed), "fixed") {
		t.Fatalf("fixed: %s, %v", fixed, err)
	}
}

type sequenceDetector struct {
	spec    sdk.DetectorSpec
	samples [][]sdk.Finding
	errors  map[int]error
	calls   int
}

func (d *sequenceDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d *sequenceDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	index := d.calls
	d.calls++
	if err := d.errors[index]; err != nil {
		return nil, err
	}
	if index >= len(d.samples) {
		return d.samples[len(d.samples)-1], nil
	}
	return d.samples[index], nil
}

type staticProvider struct{ snapshot sdk.DetectionContext }

func (p staticProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return p.snapshot, nil
}

type recordingDispatcher struct {
	requests chan IncidentRequest
	mu       sync.Mutex
	calls    int
}

type retryingDispatcher struct {
	requests chan IncidentRequest
	mu       sync.Mutex
	calls    int
}

type blockingDispatcher struct {
	requests chan IncidentRequest
	release  chan struct{}
}

func (d *blockingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	<-d.release
	return completedResult(request.IncidentID), nil
}

func (d *retryingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.mu.Unlock()
	d.requests <- request
	if call == 1 {
		return IncidentResult{}, errors.New("transient result watch failure")
	}
	return completedResult(request.IncidentID), nil
}

func (d *recordingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	d.requests <- request
	return completedResult(request.IncidentID), nil
}

func (d *recordingDispatcher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func TestControllerSuppressesTransientAndBatchesPersistentFindingsExactlyOnce(t *testing.T) {
	interval := time.Second
	first := controllerDetector("first", interval, stateFinding("b"), stateFinding("b"), stateFinding("b"))
	second := controllerDetector("second", interval, stateFinding("a"), stateFinding("a"), stateFinding("a"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 2)}
	controller, err := NewController(ControllerConfig{
		Application: "demo", Namespace: "demo", SourceCommit: "source", DeployedCommit: "deployed",
		RepositoryWorktree: "/tmp/demo",
		ResponseTimeout:    time.Minute, FiringThreshold: 2, ClearThreshold: 2,
	}, []sdk.Detector{first, second}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}

	start := time.Unix(0, 0)
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("first step: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if err := controller.Step(context.Background(), start.Add(interval), nil); err != nil {
		t.Fatalf("second step: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if got := []string{request.Findings[0].Fingerprint, request.Findings[1].Fingerprint}; !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("persistent findings were not batched: %v", got)
	}
	if len(request.DetectorHistory) != 4 {
		t.Fatalf("expected two samples per detector, got %#v", request.DetectorHistory)
	}

	if err := controller.Step(context.Background(), start.Add(2*interval), nil); err != nil {
		t.Fatalf("third step: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if dispatcher.callCount() != 1 {
		t.Fatalf("expected exactly one dispatch, got %d", dispatcher.callCount())
	}
}

func TestControllerSurfacesDetectorPlaybooksWhenFindingOmitsThem(t *testing.T) {
	start := time.Unix(0, 0)
	detector := controllerDetector("health", time.Second, stateFinding("fault"))
	detector.spec.Playbooks = []string{"health-objective"}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.FiringThreshold = 1
	controller, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("step: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.SurfacedPlaybooks) != 1 || request.SurfacedPlaybooks[0].PlaybookID != "health-objective" {
		t.Fatalf("detector playbook was not surfaced: %#v", request.SurfacedPlaybooks)
	}
}

func TestDispatchErrorRetriesTheSameIncidentInsteadOfOpeningADuplicate(t *testing.T) {
	interval := time.Second
	detector := controllerDetector("health", interval, stateFinding("fault"), stateFinding("fault"), stateFinding("fault"))
	dispatcher := &retryingDispatcher{requests: make(chan IncidentRequest, 2)}
	config := testControllerConfig()
	config.DispatchRetry = DispatchRetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	controller, err := NewController(
		config,
		[]sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	now := time.Unix(1000, 0).UTC()
	controller.now = func() time.Time { return now }
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	first := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("process failed dispatch: %v", err)
	}
	controller.handleDispatchCompletion(dispatchCompletion{
		incidentID: "stale-incident",
		result:     completedResult("stale-incident"),
	}, time.Unix(3, 0))
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("a transient dispatch failure retried without backoff")
	}
	// The bounded backoff (capped at 1ms here) is over; the retry stays pending.
	now = now.Add(2 * time.Millisecond)
	retry, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("transient dispatch failure did not remain pending for retry")
	}
	if retry.Request.IncidentID != first.IncidentID {
		t.Fatalf("dispatch retry changed incident id: got %q want %q", retry.Request.IncidentID, first.IncidentID)
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), retry); err != nil {
		t.Fatalf("execute dispatch retry: %v", err)
	}
	second := awaitRequest(t, dispatcher.requests)
	if second.IncidentID != first.IncidentID {
		t.Fatalf("dispatch retry opened duplicate incident: first=%q second=%q", first.IncidentID, second.IncidentID)
	}
}

func TestClearRefireWhileResponderRunsMergesEvidenceIntoTheSameIncident(t *testing.T) {
	interval := time.Second
	initial := stateFinding("fault")
	initial.Evidence = "readiness probe is failing"
	recurrence := stateFinding("fault")
	recurrence.Evidence = "readiness probe re-fired during rollout"
	detector := controllerDetector(
		"health",
		interval,
		initial,
		initial,
		sdk.Finding{},
		sdk.Finding{},
		recurrence,
		recurrence,
	)
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 2), release: make(chan struct{})}
	controller, err := NewController(
		testControllerConfig(),
		[]sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("initial firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	first := awaitRequest(t, dispatcher.requests)
	for sample := 2; sample < 6; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("rollout transition step: %v", err)
		}
	}
	state := exportState(t, controller)
	if !state.IncidentOpen || state.IncidentRequest == nil || state.IncidentRequest.IncidentId != first.IncidentID {
		t.Fatalf("clear/re-fire released the original incident lock: %#v", state)
	}
	if len(state.IncidentRequest.Findings) != 1 || state.IncidentRequest.Findings[0].Evidence != recurrence.Evidence {
		t.Fatalf("re-fired evidence was not merged into the open incident: %#v", state.IncidentRequest.Findings)
	}
	if pending := controller.batcher.Snapshot().Findings; len(pending) != 0 {
		t.Fatalf("re-fired evidence was buffered as a duplicate incident: %#v", pending)
	}
	assertNoRequest(t, dispatcher.requests)
	close(dispatcher.release)
}

func TestDifferentFingerprintCannotDispatchWhileResponderRuns(t *testing.T) {
	start := time.Unix(0, 0)
	first := stateFinding("selector-mismatch")
	second := stateFinding("no-ready-endpoints")
	detector := controllerDetector("health", time.Hour, first, second)
	detector.spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service", Namespace: "demo"}}
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 2), release: make(chan struct{})}
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	controller, err := NewController(
		config,
		[]sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		start,
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("initial firing step: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)

	event := sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "demo"}
	if err := controller.Step(context.Background(), start.Add(time.Millisecond), &event); err != nil {
		t.Fatalf("fingerprint transition step: %v", err)
	}
	if err := controller.Step(context.Background(), start.Add(2*time.Millisecond), nil); err != nil {
		t.Fatalf("idle scheduler step: %v", err)
	}

	state := exportState(t, controller)
	if !state.IncidentOpen || state.IncidentRequest == nil || state.IncidentRequest.IncidentId != request.IncidentID {
		t.Fatalf("queued fingerprint transition replaced the open incident: %#v", state)
	}
	if effect, ok := controller.PendingDispatchEffect(); ok {
		t.Fatalf("queued fingerprint transition became dispatchable during the open incident: %#v", effect)
	}
	close(dispatcher.release)
}

func TestControllerDebouncesFindingsFromSeparateWatchEvents(t *testing.T) {
	start := time.Unix(0, 0)
	first := controllerDetector("first", time.Minute, sdk.Finding{}, stateFinding("a"))
	first.spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}}
	second := controllerDetector("second", time.Minute, sdk.Finding{}, stateFinding("b"))
	second.spec.Watches = []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo"}}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.BatchDebounce = 100 * time.Millisecond
	controller, err := NewController(config, []sdk.Detector{first, second}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("initial step: %v", err)
	}
	configMapEvent := sdk.WatchKind{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo"}
	if err := controller.Step(context.Background(), start.Add(10*time.Millisecond), &configMapEvent); err != nil {
		t.Fatalf("ConfigMap event: %v", err)
	}
	deploymentEvent := sdk.WatchKind{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo"}
	if err := controller.Step(context.Background(), start.Add(50*time.Millisecond), &deploymentEvent); err != nil {
		t.Fatalf("Deployment event: %v", err)
	}
	assertNoRequest(t, dispatcher.requests)
	if err := controller.Step(context.Background(), start.Add(110*time.Millisecond), nil); err != nil {
		t.Fatalf("debounce deadline: %v", err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if got := []string{request.Findings[0].Fingerprint, request.Findings[1].Fingerprint}; !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("separate events were not batched: %v", got)
	}
}

func TestControllerDropsFindingThatClearsBeforeDebounceDeadline(t *testing.T) {
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Second
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	detector := controllerDetector("fault", 500*time.Millisecond, stateFinding("transient"), sdk.Finding{})
	controller, err := NewController(
		config,
		[]sdk.Detector{detector},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		start,
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("firing sample: %v", err)
	}
	if err := controller.Step(context.Background(), start.Add(500*time.Millisecond), nil); err != nil {
		t.Fatalf("clear sample: %v", err)
	}
	if err := controller.Step(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatalf("debounce deadline: %v", err)
	}
	if _, ok := controller.PendingDispatchEffect(); ok {
		t.Fatal("cleared finding survived pending debounce and became dispatchable")
	}
	assertNoRequest(t, dispatcher.requests)
}

func TestControllerIsolatesDetectorErrors(t *testing.T) {
	interval := time.Second
	broken := controllerDetector("broken", interval, sdk.Finding{}, sdk.Finding{})
	broken.errors = map[int]error{0: errors.New("broken detector"), 1: errors.New("still broken")}
	healthy := controllerDetector("healthy", interval, stateFinding("fault"), stateFinding("fault"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	var reported []error
	controller, err := NewController(testControllerConfig(), []sdk.Detector{broken, healthy}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.OnError = func(err error) { reported = append(reported, err) }

	if err := controller.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatalf("first step: %v", err)
	}
	if err := controller.Step(context.Background(), time.Unix(1, 0), nil); err != nil {
		t.Fatalf("second step: %v", err)
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	if len(reported) != 2 {
		t.Fatalf("expected isolated detector errors, got %v", reported)
	}
}

func TestResponderCompletionDoesNotCloseIncidentUntilAllHealthFindingsClear(t *testing.T) {
	interval := time.Second
	cause := controllerDetector("cause", interval, stateFinding("cause"), stateFinding("cause"), sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
	health := controllerDetector("health", interval, stateFinding("health"), stateFinding("health"), stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{})
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	health.spec.OriginatingCommit = "health-objective"
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(testControllerConfig(), []sdk.Detector{cause, health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	for sample := 2; sample < 4; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("cause clear step: %v", err)
		}
	}
	if !controller.IncidentOpen() {
		t.Fatal("responder completion closed incident while independent health finding remained")
	}
	state := exportState(t, controller)
	if state.IncidentResult == nil || controllerTime(state.ResponderCompletedAt).IsZero() || controllerTime(state.IncidentDispatchedAt).IsZero() {
		t.Fatalf("open incident did not persist responder result and timestamps: %#v", state)
	}
	select {
	case closure := <-closed:
		t.Fatalf("closure emitted before health verification: %#v", closure)
	default:
	}
	for sample := 4; sample < 6; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("health clear step: %v", err)
		}
	}
	if controller.IncidentOpen() {
		t.Fatal("incident remained open after responder completion and all findings cleared")
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("verified incident did not emit authoritative closure")
	}
	if closure.Result == nil || closure.Result.IncidentID != closure.Request.IncidentID {
		t.Fatalf("closure did not retain responder result: %#v", closure)
	}
	if len(closure.FinalDetectorStates) != 1 || closure.FinalDetectorStates[0].DetectorID != "health" ||
		closure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
		t.Fatalf("closure did not record final health state: %#v", closure.FinalDetectorStates)
	}
	if len(closure.IncidentDetectorStates) != 1 || closure.IncidentDetectorStates[0].DetectorID != "cause" ||
		closure.IncidentDetectorStates[0].Status != DetectorEvaluationClear ||
		closure.IncidentDetectorStates[0].EvaluatedAt.Before(closure.ResponderCompletedAt) {
		t.Fatalf("closure did not record the post-response incident detector state: %#v", closure.IncidentDetectorStates)
	}
	if closure.DetectedAt.IsZero() || closure.DispatchedAt.Before(closure.DetectedAt) ||
		closure.ResponderCompletedAt.Before(closure.DispatchedAt) || closure.VerifiedAt.Before(closure.ResponderCompletedAt) {
		t.Fatalf("closure timestamps are not ordered: %#v", closure)
	}
	pending, ok := controller.PendingIncidentClosure()
	if !ok || pending.Request.IncidentID != closure.Request.IncidentID {
		t.Fatalf("verified closure was not persisted for outcome handling: %#v", pending)
	}
}

func TestControllerClosesAsFailedWhenPostResponseHealthNeverClears(t *testing.T) {
	interval := time.Second
	health := controllerDetector(
		"health", interval,
		stateFinding("health"), stateFinding("health"), stateFinding("health"), stateFinding("health"),
	)
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	health.spec.OriginatingCommit = "health-objective"
	for index := range health.samples {
		health.samples[index][0].Severity = sdk.SeverityCritical
	}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.VerificationTimeout = 2 * time.Second
	controller, err := NewController(
		config, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("completion step: %v", err)
	}
	select {
	case <-closed:
		t.Fatal("incident closed before the verification window elapsed")
	default:
	}
	if err := controller.Step(context.Background(), time.Unix(4, 0), nil); err != nil {
		t.Fatalf("verification deadline step: %v", err)
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("persistent post-response health finding did not close the incident at the timeout")
	}
	if len(closure.FinalDetectorStates) != 0 || closure.HealthClearedAt != nil {
		t.Fatalf("timed-out closure claims verified health: %#v", closure)
	}
	if closure.VerifiedAt.Before(closure.ResponderCompletedAt.Add(config.VerificationTimeout)) {
		t.Fatalf("closure was cut before the verification window elapsed: %#v", closure)
	}
	if controller.IncidentOpen() {
		t.Fatal("timed-out incident stayed open")
	}
}

type failedJobDispatcher struct {
	mu    sync.Mutex
	calls int
}

func (d *failedJobDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return IncidentResult{}, &ResponderJobFailedError{JobName: IncidentJobName(request.IncidentID)}
}

// A responder Job that failed (its pod was killed, say) is terminal: the
// dispatcher rejoins the same Job by name, so retrying it hot-looped forever
// and the incident could never close. The controller must record the failure
// once and let health decide the closure.
func TestAFailedResponderJobIsATerminalDispatchFailure(t *testing.T) {
	interval := time.Second
	health := controllerDetector(
		"health", interval, stateFinding("health"), stateFinding("health"), sdk.Finding{}, sdk.Finding{},
	)
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	health.spec.OriginatingCommit = "health-objective"
	for index := range health.samples {
		if len(health.samples[index]) > 0 {
			health.samples[index][0].Severity = sdk.SeverityCritical
		}
	}
	dispatcher := &failedJobDispatcher{}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher,
		time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("completion step: %v", err)
	}
	if _, pending := controller.PendingDispatchEffect(); pending {
		t.Fatal("a failed responder Job was scheduled for another dispatch")
	}
	if err := controller.Step(context.Background(), time.Unix(3, 0), nil); err != nil {
		t.Fatalf("clear step: %v", err)
	}
	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatalf("incident with a failed responder never closed: %#v", exportState(t, controller))
	}
	if closure.Result != nil || !strings.Contains(closure.DispatchError, "failed") {
		t.Fatalf("closure must record the terminal dispatch failure: %#v", closure)
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.calls != 1 {
		t.Fatalf("failed responder Job dispatched %d times", dispatcher.calls)
	}
}

func controllerDetector(id string, interval time.Duration, samples ...sdk.Finding) *sequenceDetector {
	sequences := make([][]sdk.Finding, 0, len(samples))
	for _, finding := range samples {
		if finding.RuleID == "" {
			sequences = append(sequences, nil)
			continue
		}
		finding.DetectorID = id
		sequences = append(sequences, []sdk.Finding{finding})
	}
	return &sequenceDetector{
		spec: sdk.DetectorSpec{ID: id, Interval: interval}, samples: sequences, errors: make(map[int]error),
	}
}

func testControllerConfig() ControllerConfig {
	return ControllerConfig{
		Application: "demo", Namespace: "demo", SourceCommit: "source", DeployedCommit: "deployed",
		RepositoryWorktree: "/tmp/demo",
		ResponseTimeout:    time.Minute, FiringThreshold: 2, ClearThreshold: 2,
	}
}

func completedResult(incidentID string) IncidentResult {
	now := time.Now().UTC()
	return IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: incidentID, Status: IncidentCompleted,
		Usage: UsageMetrics{}, Timing: TimingMetrics{StartedAt: now, CompletedAt: now},
	}
}

func awaitRequest(t *testing.T, requests <-chan IncidentRequest) IncidentRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatch")
		return IncidentRequest{}
	}
}

func awaitDispatchCompletionQueued(t *testing.T, controller *Controller) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(controller.results) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for dispatch completion")
}

func assertNoRequest(t *testing.T, requests <-chan IncidentRequest) {
	t.Helper()
	select {
	case request := <-requests:
		t.Fatalf("unexpected dispatch: %#v", request)
	case <-time.After(20 * time.Millisecond):
	}
}

func executePendingEffect(t *testing.T, controller *Controller) {
	t.Helper()
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch effect")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatalf("execute dispatch effect: %v", err)
	}
}

func (d *sequenceDetector) String() string { return fmt.Sprintf("detector(%s)", d.spec.ID) }

func TestControllerEvaluateAllRunsEveryDetectorRegardlessOfSchedule(t *testing.T) {
	slow := controllerDetector("slow", time.Hour, sdk.Finding{})
	fast := controllerDetector("fast", time.Hour, sdk.Finding{})
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(
		testControllerConfig(), []sdk.Detector{slow, fast},
		staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.Step(context.Background(), time.Unix(0, 0), nil); err != nil {
		t.Fatalf("initial step: %v", err)
	}
	if err := controller.Step(context.Background(), time.Unix(10, 0), nil); err != nil {
		t.Fatalf("idle step: %v", err)
	}
	if slow.calls != 1 || fast.calls != 1 {
		t.Fatalf("detectors ran before their interval: slow=%d fast=%d", slow.calls, fast.calls)
	}
	var evaluated []sdk.Finding
	evaluations := 0
	controller.OnEvaluation = func(findings []sdk.Finding) {
		evaluations++
		evaluated = findings
	}
	if err := controller.EvaluateAll(context.Background(), time.Unix(11, 0)); err != nil {
		t.Fatalf("evaluate all: %v", err)
	}
	if slow.calls != 2 || fast.calls != 2 || evaluations != 1 || len(evaluated) != 0 {
		t.Fatalf("resume evaluation must run every detector once: slow=%d fast=%d evaluations=%d", slow.calls, fast.calls, evaluations)
	}
}
