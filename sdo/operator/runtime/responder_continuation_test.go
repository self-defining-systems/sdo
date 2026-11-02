package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type continuationDetector struct{ healthy *atomic.Bool }

func (d continuationDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "health-objective"}
}

func (d continuationDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.healthy.Load() {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: "health-objective", RuleID: "unavailable", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Scripted availability remains unhealthy", Evidence: "Scripted health observation is false", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "api"}}}, nil
}

func publishContinuationResult(t *testing.T, client *fake.Clientset, job *batchv1.Job, result IncidentResult) {
	t.Helper()
	ctx := context.Background()
	cm, err := client.CoreV1().ConfigMaps("demo").Get(ctx, job.Name+"-request", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	task := new(contracts.AgentTask)
	if err := protojson.Unmarshal([]byte(cm.Data[jobRequestDataKey]), task); err != nil {
		t.Fatal(err)
	}
	typed, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := anypb.New(typed)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.Canonical(&contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: string(result.Status), Response: packed})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result"}, Data: map[string]string{jobResultDataKey: string(payload)}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err = client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func continuationLoop(r IncidentRequest, healthy *atomic.Bool, budget int) *ResponderLoop {
	loop := NewResponderLoop(r, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: r.Namespace}}, []sdk.Detector{continuationDetector{healthy}}, PlanExecutor{}, budget)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	return loop
}

type verificationRetryDispatcher struct {
	requests chan IncidentRequest
	resumed  int
}

func (d *verificationRetryDispatcher) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	d.requests <- r
	return completedResult(r.IncidentID), nil
}

func (d *verificationRetryDispatcher) ContinueAfterVerification(_ context.Context, _ IncidentRequest, _ ToolResponse, _ time.Time) (bool, error) {
	d.resumed++
	return true, nil
}

func TestControllerResumesAfterUnhealthyIndependentVerification(t *testing.T) {
	start := time.Unix(0, 0)
	cfg := testControllerConfig()
	cfg.FiringThreshold = 1
	cfg.BatchDebounce = 0
	detector := controllerDetector("health", time.Second, stateFinding("bad"), stateFinding("bad"), stateFinding("bad"))
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	d := &verificationRetryDispatcher{requests: make(chan IncidentRequest, 2)}
	c, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, d, start)
	if err != nil {
		t.Fatal(err)
	}
	// Pending dispatch now checks the frozen verification bound against its
	// clock; keep that clock on the same synthetic timeline as Step.
	c.now = func() time.Time { return start.Add(2 * time.Second) }
	for sample := 0; sample < 2; sample++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(sample)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, c)
	first := awaitRequest(t, d.requests)
	awaitDispatchCompletionQueued(t, c)
	if err := c.Step(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if d.resumed != 1 {
		t.Fatal("unhealthy controller verification did not requeue responder within original budget")
	}
	effect, pending := c.PendingDispatchEffect()
	if !pending || effect.Request.IncidentID != first.IncidentID || !effect.Request.ResponseDeadline.Equal(first.ResponseDeadline) {
		t.Fatal("continuation changed incident or original deadline")
	}
	if c.verificationDeadlineLocked().IsZero() || c.verificationDeadlineLocked().After(first.ResponseDeadline) {
		t.Fatal("continuation lost its frozen verification bound")
	}
}

func TestContinuationUsagePreservesUnknownFieldsAndDeduplicatesRepairs(t *testing.T) {
	first := completedResult("incident")
	first.RepairActions = []RepairActionReceipt{{ActionID: "first", Success: true}}
	first.Usage.InputTokens = 100
	first.Usage.TotalCostUsd = proto.Float64(.1)
	typed, err := incidentResultContract(first)
	if err != nil {
		t.Fatal(err)
	}
	second := completedResult("incident")
	second.RepairActions = []RepairActionReceipt{{ActionID: "first", Success: true}, {ActionID: "second", Success: true}}
	second.Usage.InputTokens = 200
	got := accumulateResponderResult(typed, second)
	if got.Usage.InputTokens != 300 || got.Usage.TotalCostUsd != nil || len(got.RepairActions) != 2 {
		t.Fatalf("partial usage or duplicate actions: %+v", got)
	}
}

func TestControllerWaitsForFreshClearSamplesInsteadOfRedispatch(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	snapshot := sdktest.Snapshot{NamespaceName: "demo"}
	d := &verificationRetryDispatcher{requests: make(chan IncidentRequest, 1)}
	c, err := NewController(testControllerConfig(), []sdk.Detector{controllerDetector("health-objective", time.Second, sdk.Finding{})}, staticProvider{snapshot: snapshot}, d, now)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.ObservationGeneration = observationGeneration(snapshot)
	request.ResponseDeadline = now.Add(time.Minute)
	c.currentIncidentRequest = &request
	result := completedResult(request.IncidentID)
	c.currentIncidentResult = &result
	c.incidentOpen, c.responderDone = true, true
	c.responderCompletedAt = now
	c.healthDetectorIDs = []string{"health-objective"}
	c.history = []DetectorEvaluation{{DetectorID: "health-objective", EvaluatedAt: now.Add(-time.Second), Status: DetectorEvaluationClear}}
	c.maybeCloseIncident(context.Background(), now, snapshot)
	if d.resumed != 0 || !c.responderDone || !c.responderCompletedAt.Equal(now) {
		t.Fatal("fresh initial health observation caused another worker or reset response completion")
	}
	// A fresh sample is not itself a stability proof. Drive the real detector
	// owner across the complete window rather than inventing clear history.
	assertRecoveryFixtureWindow(t, c, context.Background(), now)
	if d.resumed != 0 || c.IncidentOpen() {
		t.Fatal("fresh stable health redispatched or failed to close")
	}
}

func TestContinuationFeedbackNamesDriftAndSharesSchema(t *testing.T) {
	finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Fingerprint: "flags/key", Summary: "accepted source difference", Evidence: "field data.fail differs", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "demo", Name: "flags"}}
	response := ToolResponse{Healthy: false, Summary: "unresolved accepted drift", Findings: []sdk.Finding{finding}}
	encoded, err := continuationFeedback(2, response)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"attempt", "healthy", "summary", "still_broken", "already_done"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("missing shared feedback field %s: %s", key, encoded)
		}
	}
	if _, ok := payload["detector_states"]; ok {
		t.Fatal("independent feedback used divergent schema")
	}
	if !strings.Contains(string(encoded), "flags/key") || !strings.Contains(string(encoded), "ConfigMap") || !strings.Contains(string(encoded), "flags") {
		t.Fatalf("drift identity lost: %s", encoded)
	}
}

func TestControllerContinuationDistinguishesFaultFromObservationWindow(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.healthDetectorIDs = []string{"health"}
	c.responderCompletedAt = now
	c.history = []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(-time.Second), Status: DetectorEvaluationClear}}
	pending := c.continuationVerificationLocked(false, closureSourceReport{})
	if len(pending.Findings) != 0 || pending.Error != "" || !strings.Contains(pending.Summary, "health") || !strings.Contains(pending.Summary, now.Format(time.RFC3339Nano)) {
		t.Fatalf("unnamed pending observation: %+v", pending)
	}
	drift := sdk.Finding{DetectorID: "drift.unexplained", PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "demo", Name: "flags"}, Fingerprint: "flags/key"}
	// Drift is information: it is shown but never holds the verdict.
	informed := c.continuationVerificationLocked(true, closureSourceReport{Drift: []sdk.Finding{drift}})
	if !informed.Healthy || len(informed.Findings) != 0 || len(informed.Drift) != 1 || informed.Drift[0].PrimaryResource.Name != "flags" {
		t.Fatalf("drift changed the health verdict or was hidden: %+v", informed)
	}
	// A healthy verdict with remaining drift must not read as finished work.
	if !strings.Contains(informed.Summary, "1 object(s) still differ from the accepted source") || strings.Contains(informed.Summary, "information only") || !strings.Contains(informed.Summary, "restore each or explain why it is intended") {
		t.Fatalf("healthy summary with drift reads as done: %q", informed.Summary)
	}
	failed := c.continuationVerificationLocked(false, closureSourceReport{Drift: []sdk.Finding{drift}})
	if failed.Healthy {
		t.Fatalf("drift made unhealthy state healthy: %+v", failed)
	}
	c.history = append(c.history, DetectorEvaluation{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationError, Error: "snapshot unavailable"})
	failed = c.continuationVerificationLocked(false, closureSourceReport{})
	if !strings.Contains(failed.Error, "health: snapshot unavailable") {
		t.Fatalf("unnamed detector failure: %+v", failed)
	}
}

// reentrantContinuer stands in for the production LoopFactory, which registers
// the responder loop with the controller under the controller's own mutex.
type reentrantContinuer struct {
	verificationRetryDispatcher
	controller *Controller
	refuse     bool
}

func (d *reentrantContinuer) ContinueAfterVerification(ctx context.Context, request IncidentRequest, verification ToolResponse, now time.Time) (bool, error) {
	d.controller.mu.Lock()
	d.controller.mu.Unlock()
	if d.refuse {
		return false, nil
	}
	return d.verificationRetryDispatcher.ContinueAfterVerification(ctx, request, verification, now)
}

func mustExportState(t *testing.T, c *Controller) *contracts.ControllerState {
	t.Helper()
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// clearingContinuer drops the incident while the continuation runs, which
// observationMu makes impossible in production; the controller must then
// abandon the continuation instead of reopening a closed incident.
type clearingContinuer struct {
	verificationRetryDispatcher
	controller *Controller
}

func (d *clearingContinuer) ContinueAfterVerification(context.Context, IncidentRequest, ToolResponse, time.Time) (bool, error) {
	d.controller.mu.Lock()
	d.controller.incidentOpen = false
	d.controller.mu.Unlock()
	return true, nil
}

func TestControllerAbandonsContinuationWhenTheIncidentChangedDuringIt(t *testing.T) {
	start := time.Unix(0, 0)
	cfg := testControllerConfig()
	cfg.FiringThreshold = 1
	cfg.BatchDebounce = 0
	detector := controllerDetector("health", time.Second, stateFinding("bad"), stateFinding("bad"), stateFinding("bad"), stateFinding("bad"))
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	d := &clearingContinuer{verificationRetryDispatcher: verificationRetryDispatcher{requests: make(chan IncidentRequest, 2)}}
	c, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, d, start)
	if err != nil {
		t.Fatal(err)
	}
	d.controller = c
	for sample := 0; sample < 2; sample++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(sample)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, c)
	awaitRequest(t, d.requests)
	awaitDispatchCompletionQueued(t, c)
	if err := c.Step(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.responderDone || c.dispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("a continuation for a changed incident was applied: responderDone=%v dispatch=%v", c.responderDone, c.dispatchState)
	}
}

type failingSnapshotProvider struct{}

func (failingSnapshotProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return nil, errors.New("api server unavailable")
}

// The eval calls a controller hung when step_count stops, so the heartbeat
// must keep moving while snapshots fail and evaluation_iteration stands still.
func TestStepCountAdvancesWhileSnapshotsFailAndEvaluationsStop(t *testing.T) {
	start := time.Unix(0, 0)
	detector := controllerDetector("health", time.Second, stateFinding("bad"))
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, failingSnapshotProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	before := mustExportState(t, c)
	for sample := 0; sample < 3; sample++ {
		if err := c.Step(context.Background(), start.Add(time.Duration(sample)*time.Second), nil); err == nil {
			t.Fatal("snapshot failure was not reported")
		}
	}
	after := mustExportState(t, c)
	if after.GetStepCount()-before.GetStepCount() != 3 {
		t.Fatalf("heartbeat missed failing steps: %d -> %d", before.GetStepCount(), after.GetStepCount())
	}
	if after.GetEvaluationIteration() != before.GetEvaluationIteration() {
		t.Fatalf("evaluation advanced without a snapshot: %d -> %d", before.GetEvaluationIteration(), after.GetEvaluationIteration())
	}
}

