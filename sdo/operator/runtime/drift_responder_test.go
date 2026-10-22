package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
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

func TestClosureRetainsDriftWithHealthyDetectors(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	f := unexplainedFinding()
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "incident", Findings: []sdk.Finding{f}}
	c.incidentOpen = true
	c.incidentFindingKeys = findingKeys([]sdk.Finding{f})
	c.responderCompletedAt = start
	c.healthDetectorIDs = []string{"health"}
	c.recordEvaluation("health", start.Add(time.Second), nil)
	c.recordEvaluation(f.DetectorID, start.Add(time.Second), []sdk.Finding{f})
	if _, clear := c.finalVerificationStates(); clear {
		t.Fatal("quiet health closed retained drift")
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
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
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
	c.SetFiringSink(sink)
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
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
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, BatchWindow: time.Second, View: func(snapshot sdk.DetectionContext) program.ReadView {
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

func TestVerifyRetainedLearnedDriftRequiresEveryBinding(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	other := ref
	other.Name = "peer"
	first := sdk.Finding{DetectorID: "drift.targetport", RuleID: "targetport", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: program.Bindings{"service": ref}}
	second := first
	second.PrimaryResource = other
	second.ParameterBindings = program.Bindings{"service": other}
	for _, result := range []program.Result{program.Match, program.Unknown, program.NoMatch} {
		t.Run(string(result), func(t *testing.T) {
			registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "targetport", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}, run: func(_ context.Context, b program.Bindings) program.Verdict {
				if b["service"].Name == "api" {
					return program.Verdict{Result: program.NoMatch}
				}
				return program.Verdict{Result: result}
			}}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "incident", Findings: []sdk.Finding{first, second}}, registry, staticProvider{snapshot: sdktest.Snapshot{}}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{}, 1)
			loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
			verify := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
			if verify.Healthy != (result == program.NoMatch) {
				t.Fatalf("binding-specific clearance lost: %+v", verify)
			}
			if result != program.NoMatch && (len(verify.Findings) != 1 || verify.Findings[0].PrimaryResource != other) {
				t.Fatalf("wrong binding remained: %+v", verify.Findings)
			}
		})
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
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}, Classification: sdk.DriftUnexplained, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999", Source: "8080"}}}
	c.queueUnexplainedFinding(start, event)
	event.Fields = []sdk.DriftField{{Path: "spec.selector.app", New: "wrong", Source: "api"}}
	c.queueUnexplainedFinding(start, event)
	pending := c.batcher.Snapshot().Findings
	if len(pending) != 2 || FindingFingerprint(pending[0]) == FindingFingerprint(pending[1]) {
		t.Fatal("independent retained field sets collapsed into one finding")
	}
}
