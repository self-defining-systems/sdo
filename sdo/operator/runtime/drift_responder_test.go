package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/latefindings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func unexplainedFinding() sdk.Finding {
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Classification: sdk.DriftUnexplained}
	f := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityLatent, PrimaryResource: event.Ref, Metadata: map[string]any{"drift_event": event}}
	f.Fingerprint = FindingFingerprint(f)
	return f
}

func TestVerifyRetainsDriftWithHealthyDetectors(t *testing.T) {
	f := unexplainedFinding()
	health := controllerDetector("health", time.Second, sdk.Finding{})
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Namespace: "app", Findings: []sdk.Finding{f}}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{health}, PlanExecutor{}, 1)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}
	for _, command := range []string{"status", "diagnose", "verify"} {
		result := loop.Handle(context.Background(), ToolRequest{Command: command})
		if result.Healthy || len(result.Findings) != 1 {
			t.Fatalf("%s lost retained drift with quiet health: %+v", command, result)
		}
	}
}

func TestRepairedDriftRecursWithoutProgram(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	before, broken := driftService(8080), driftService(9999)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*broken}}}
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	c, err := NewController(config, nil, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: snapshot.(sdktest.Snapshot).ServiceList[0].Spec.Ports[0].TargetPort.IntVal == 9999}}
	}})
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
	if err := c.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := c.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !c.IncidentOpen() {
		t.Fatal("unexplained drift never opened an ordinary responder incident")
	}
}

func TestUnexplainedDriftDuringResponseAttachesToOpenIncident(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	before, broken := driftService(8080), driftService(9999)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(dispatcher.release)
	c, err := NewController(config, []sdk.Detector{controllerDetector("health", time.Second, stateFinding("health"))}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*broken}}}, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	lateClient := fake.NewSimpleClientset()
	lateStore := latefindings.Store{Client: lateClient, Namespace: "control"}
	c.SetFiringSink(lateFindingSink{Downstream: sink, Store: lateStore})
	c.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}})
	if err := c.StepEvents(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, c)
	first := awaitRequest(t, dispatcher.requests)
	if err := lateStore.Ensure(context.Background(), first.IncidentID); err != nil {
		t.Fatal(err)
	}
	frozen := exportState(t, c).IncidentRequest.PreparedAgentTask
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
	if err := c.StepEvents(context.Background(), start.Add(2*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, c)
	if state.IncidentRequest.IncidentId != first.IncidentID || len(state.IncidentRequest.DriftEvents) != 1 || len(state.PendingBatch.Findings) != 0 || state.IncidentRequest.PreparedAgentTask != frozen {
		t.Fatalf("late drift created another request or lost evidence: %+v", state)
	}
	late := 0
	for _, record := range sink.records {
		if record.GetDetectorId() == "drift.unexplained" && record.GetEvent() == "activated" {
			late++
			if record.GetDispatchRelation() != "after_dispatch" || record.GetIncidentId() != first.IncidentID {
				t.Fatalf("late drift not published: %+v", record)
			}
		}
	}
	cm, err := lateClient.CoreV1().ConfigMaps("control").Get(context.Background(), latefindings.ConfigMapName(first.IncidentID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	report := new(contracts.LateFindingsReport)
	if err := wire.Unmarshal([]byte(cm.Data[latefindings.ReportKey]), report); err != nil {
		t.Fatal(err)
	}
	if len(report.GetLateFindings()) != 1 || report.GetLateFindings()[0].GetDetectorId() != "drift.unexplained" {
		t.Fatalf("running responder cannot pull late drift evidence: %v", report)
	}
	if len(first.DriftEvents) != 0 || len(dispatcher.requests) != 0 {
		t.Fatal("late drift changed the frozen task or started another responder")
	}
	if late != 1 {
		t.Fatalf("late activations=%d", late)
	}
	if state.IncidentView == nil || len(state.IncidentView.DriftEvents) != 1 {
		t.Fatal("late drift missing from moving view")
	}
}

func TestUnexplainedTargetPortOpensLatentIncidentWithDriftCorpus(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	root := t.TempDir()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	broken := driftService(9999)
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = root
	c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*broken}}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Wait)
	c.ConfigureDrift(DriftControllerConfig{Events: cache, BatchWindow: time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}})
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(driftService(8080), broken)
	if err := c.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := c.StepEvents(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	request := exportState(t, c).IncidentRequest
	if request == nil || len(request.DriftEvents) != 1 || request.Findings[0].DetectorId != "drift.unexplained" {
		t.Fatalf("drift did not enter ordinary responder: %+v", request)
	}
	dir, err := memory.IncidentCorpusDir(root, request.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil || snapshot.ServiceList[0].Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatal("original broken capture missing")
	}
}

type failedSourceDriftView struct{ driftReadView }

func (failedSourceDriftView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, fmt.Errorf("source unavailable")
}
func TestUnexplainedDriftSourceErrorNeverClears(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Findings: []sdk.Finding{unexplainedFinding()}}, nil, staticProvider{snapshot: sdktest.Snapshot{}}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{}, 1)
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return failedSourceDriftView{} }
	result := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if result.Healthy || len(result.Findings) != 1 || result.DetectorStates[len(result.DetectorStates)-1].Status != DetectorEvaluationError {
		t.Fatalf("source error cleared drift: %+v", result)
	}
}

func TestUnexplainedDriftRetainsIndependentFieldSetsOnSameObject(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Classification: sdk.DriftUnexplained, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999", Source: "8080"}}}
	c.queueUnexplainedFinding(start, event)
	event.Fields = []sdk.DriftField{{Path: "spec.selector.app", New: "wrong", Source: "api"}}
	c.queueUnexplainedFinding(start, event)
	pending := c.batcher.Snapshot().Findings
	if len(pending) != 2 || FindingFingerprint(pending[0]) == FindingFingerprint(pending[1]) {
		t.Fatal("independent retained field sets collapsed into one finding")
	}
}

// pendingDriftController queues one unexplained drift finding behind a 20s
// batch window at start+1s while a health detector with a 500ms debounce stays
// quiet for its first sample and fires from the second.
func pendingDriftController(t *testing.T, start time.Time, health []sdk.Finding, changed *bool) (*Controller, *memorySink, *KubernetesCache) {
	t.Helper()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := testControllerConfig()
	config.Namespace = "app"
	config.FiringThreshold = 1
	var detectors []sdk.Detector
	if len(health) > 0 {
		detector := controllerDetector("health", time.Second, health...)
		detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
		detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: 500 * time.Millisecond}
		detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
		detector.spec.OriginatingCommit = "source"
		detectors = append(detectors, detector)
	}
	c, err := NewController(config, detectors, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*driftService(9999)}}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	sink := &memorySink{}
	c.SetFiringSink(sink)
	c.ConfigureDrift(DriftControllerConfig{Events: cache, BatchWindow: 20 * time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: *changed}}
	}})
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(driftService(8080), driftService(9999))
	if err := c.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	return c, sink, cache
}

// stepHealthIncident fires the health finding at start+2s and lets its 500ms
// debounce pass at start+3s, still inside the drift batch window.
func stepHealthIncident(t *testing.T, c *Controller, start time.Time) {
	t.Helper()
	for _, offset := range []time.Duration{2 * time.Second, 3 * time.Second} {
		if err := c.StepEvents(context.Background(), start.Add(offset), nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHealthIncidentCoalescesPendingUnexplainedDrift(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	changed := true
	c, sink, _ := pendingDriftController(t, start, []sdk.Finding{{}, stateFinding("health")}, &changed)
	if c.IncidentOpen() {
		t.Fatal("incident opened before the health finding fired")
	}
	stepHealthIncident(t, c, start)
	state := exportState(t, c)
	request := state.IncidentRequest
	if request == nil || len(request.Findings) != 2 || len(request.DriftEvents) != 1 || len(state.PendingBatch.Findings) != 0 {
		t.Fatalf("pending drift did not join the health incident: request=%+v pending=%+v", request, state.PendingBatch)
	}
	detectors := map[string]bool{}
	for _, finding := range request.Findings {
		detectors[finding.DetectorId] = true
	}
	if !detectors["health"] || !detectors["drift.unexplained"] {
		t.Fatalf("request findings=%v", detectors)
	}
	records := sink.events(FiringBatched, "drift.unexplained")
	if len(records) != 1 || records[0].GetDispatchRelation() != "before_dispatch" {
		t.Fatalf("drift not tracked as before_dispatch: %+v", records)
	}
}

func TestPendingDriftAloneStillWaitsForBatchWindow(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	changed := true
	c, _, _ := pendingDriftController(t, start, nil, &changed)
	if err := c.StepEvents(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if c.IncidentOpen() {
		t.Fatal("drift-only batch opened inside the batch window")
	}
	if err := c.StepEvents(context.Background(), start.Add(22*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !c.IncidentOpen() {
		t.Fatal("drift-only batch never opened after the batch window")
	}
}

func TestUnexplainedDriftFindingUsesAcceptedSourceRestorationDefault(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, now)
	if err != nil {
		t.Fatal(err)
	}
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}, Classification: sdk.DriftUnexplained}
	c.ConfigureDrift(DriftControllerConfig{})
	c.queueUnexplainedFinding(now, event)
	findings := exportState(t, c).PendingBatch.Findings
	if len(findings) != 1 {
		t.Fatalf("lost drift evidence: %+v", findings)
	}
	finding := findings[0]
	if !strings.Contains(finding.Summary, "restore accepted-source differences") || !strings.Contains(finding.Summary, "positive evidence") {
		t.Fatalf("finding still requires impact before restoration: %s", finding.Summary)
	}
	if finding.PrimaryResource.Name != event.Ref.Name || finding.DetectorId != "drift.unexplained" {
		t.Fatalf("changed drift identity: %+v", finding)
	}
}
