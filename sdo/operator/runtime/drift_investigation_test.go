package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type recordingDriftInvestigator struct {
	requests []*contracts.DriftInvestigationTask
	result   *contracts.DriftInvestigationResult
	err      error
}

type blockingDriftInvestigator struct {
	started chan struct{}
	release chan struct{}
}

func (worker blockingDriftInvestigator) Investigate(_ context.Context, _ *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error) {
	close(worker.started)
	<-worker.release
	return &contracts.DriftInvestigationResult{}, nil
}

func TestControllerDowngradesUnsupportedBugWithoutIncident(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	before, broken := driftService(8080), driftService(9999)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
	config := testControllerConfig()
	config.Namespace = "app"
	controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*broken}}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{Events: []*contracts.DriftEventInvestigation{{Verdict: "bug", Reason: "suspected"}}}}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
	}, Investigation: DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := controller.StepEvents(context.Background(), start.Add(2*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state := controller.ExportState()
	if state.IncidentOpen || len(state.DriftEvents) != 1 || state.DriftEvents[0].InvestigationVerdict != "unsure" {
		t.Fatalf("unsupported bug opened incident or lost weak label: %+v", state)
	}
}

func TestTargetPortSourceContradictionAndLivePodPortMismatchSupportBug(t *testing.T) {
	result := validateDriftVerdict(&contracts.DriftEventInvestigation{
		Verdict: "bug", Reason: "Service routes to no selected container port",
		Evidence: []*contracts.DriftEvidence{
			{Kind: "source_contradiction", Observation: "accepted Service targetPort is 8087; live targetPort is 9999"},
			{Kind: "violated_relation", Observation: "live selected reservation pods expose containerPort 8087, none expose 9999"},
		},
	})
	if result.GetVerdict() != "bug" {
		t.Fatalf("source contradiction and live relation did not support bug: %+v", result)
	}
}

func TestDriftWorkerRunsWithoutBlockingControllerStep(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := blockingDriftInvestigator{started: make(chan struct{}), release: make(chan struct{})}
	controller, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Investigation: DriftInvestigationConfig{Worker: worker, Async: true, BatchWindow: time.Second}})
	controller.driftInvestigation.Add(sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fields: []sdk.DriftField{{Path: "spec.port"}}, ObservedAt: start})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worker.started:
	case <-time.After(time.Second):
		t.Fatal("drift worker was not dispatched")
	}
	close(worker.release)
	select {
	case completion := <-controller.driftResults:
		controller.handleDriftCompletion(completion, start.Add(2*time.Second))
	case <-time.After(time.Second):
		t.Fatal("drift worker did not complete")
	}
}

func TestDriftTaskBuildFailureBacksOff(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	controller, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	errors := 0
	controller.OnError = func(error) { errors++ }
	controller.ConfigureDrift(DriftControllerConfig{Investigation: DriftInvestigationConfig{Worker: &recordingDriftInvestigator{}, Async: true, BatchWindow: time.Second, BuildTask: func(context.Context, string, []sdk.DriftEvent) (*contracts.DriftInvestigationTask, error) {
		attempts++
		return nil, fmt.Errorf("stale commit")
	}}})
	controller.driftInvestigation.Add(sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fields: []sdk.DriftField{{Path: "spec.port"}}, ObservedAt: start})
	for _, at := range []time.Time{start.Add(time.Second), start.Add(1500 * time.Millisecond), start.Add(3 * time.Second)} {
		if err := controller.StepEvents(context.Background(), at, nil); err != nil {
			t.Fatal(err)
		}
	}
	if attempts != 2 || errors != 2 {
		t.Fatalf("task builder called %d times and logged %d errors across two retry intervals", attempts, errors)
	}
}

func TestInvestigatedTargetPortOpensLatentIncidentWithDriftCorpus(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	root := t.TempDir()
	before, broken := driftService(8080), driftService(9999)
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
	provider := &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*broken}}}
	config := testControllerConfig()
	config.Namespace, config.CorpusSessionRoot, config.FiringThreshold, config.ClearThreshold = "app", root, 1, 1
	dispatcher := &fixedResultDispatcher{requests: make(chan IncidentRequest, 1), result: func(request IncidentRequest) IncidentResult { return completedResult(request.IncidentID) }}
	controller, err := NewController(config, nil, provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{Events: []*contracts.DriftEventInvestigation{{
		EventIndex: 0, Verdict: "bug", Reason: "unserved port", Evidence: []*contracts.DriftEvidence{
			{Kind: "source_contradiction", Observation: "source serves 8080, live targets 9999"},
			{Kind: "violated_relation", Observation: "no selected pod serves 9999"},
		},
	}}}}
	controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
		services := snapshot.(sdktest.Snapshot).ServiceList
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: services[0].Spec.Ports[0].TargetPort.IntVal == 9999}}
	}, Investigation: DriftInvestigationConfig{Worker: worker, BatchWindow: 20 * time.Second, DispatchesPerHour: 5,
		BuildTask: func(_ context.Context, batchID string, events []sdk.DriftEvent) (*contracts.DriftInvestigationTask, error) {
			if err := memory.CaptureIncidentSnapshot(root, batchID, "open@detect", provider.snapshot, "app"); err != nil {
				return nil, err
			}
			return &contracts.DriftInvestigationTask{SchemaVersion: ProtocolSchemaVersion, BatchId: batchID, CaptureId: batchID, Events: driftEventsContract(events)}, nil
		},
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if controller.IncidentOpen() {
		t.Fatal("incident opened before investigator")
	}
	if err := controller.StepEvents(context.Background(), start.Add(21*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 1 || !controller.IncidentOpen() {
		t.Fatalf("bug did not open latent incident: %+v", controller.ExportState())
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.Findings) != 1 || request.Findings[0].Severity != sdk.SeverityLatent || request.DriftEvents[0].InvestigationVerdict != "bug" {
		t.Fatalf("investigator evidence missing from incident: %+v", request)
	}
	from, _ := memory.IncidentCorpusDir(root, worker.requests[0].CaptureId)
	to, _ := memory.IncidentCorpusDir(root, request.IncidentID)
	opened, err := os.ReadFile(filepath.Join(from, "open@detect.json"))
	if err != nil {
		t.Fatal(err)
	}
	reused, err := os.ReadFile(filepath.Join(to, "open@detect.json"))
	if err != nil || string(opened) != string(reused) {
		t.Fatalf("drift capture was not reused: %v", err)
	}
	awaitDispatchCompletionQueued(t, controller)
	provider.snapshot = sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*before}}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(broken, before)
	if err := controller.StepEvents(context.Background(), start.Add(22*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if _, ok := controller.PendingIncidentClosure(); !ok {
		t.Fatal("verified repair did not reach normal incident closure")
	}
}

func (worker *recordingDriftInvestigator) Investigate(_ context.Context, request *contracts.DriftInvestigationTask) (*contracts.DriftInvestigationResult, error) {
	worker.requests = append(worker.requests, request)
	return worker.result, worker.err
}

func TestDriftInvestigationRequiresBothEvidenceKinds(t *testing.T) {
	for _, test := range []struct {
		name     string
		evidence []*contracts.DriftEvidence
		want     string
	}{
		{"none", nil, "unsure"},
		{"source only", []*contracts.DriftEvidence{{Kind: "source_contradiction", Observation: "source port 8080"}}, "unsure"},
		{"relation only", []*contracts.DriftEvidence{{Kind: "violated_relation", Observation: "no pod serves port"}}, "unsure"},
		{"both", []*contracts.DriftEvidence{{Kind: "source_contradiction", Observation: "source port 8080"}, {Kind: "violated_relation", Observation: "no pod serves port"}}, "bug"},
	} {
		t.Run(test.name, func(t *testing.T) {
			verdict := validateDriftVerdict(&contracts.DriftEventInvestigation{Verdict: "bug", Evidence: test.evidence})
			if verdict.GetVerdict() != test.want {
				t.Fatalf("got %q, want %q", verdict.GetVerdict(), test.want)
			}
		})
	}
}

func TestDriftInvestigationBatchesDeduplicatesAndKeepsOverBudgetPending(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{}}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: 20 * time.Second, DispatchesPerHour: 1})
	first := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", Old: "8080", New: "9999"}}, ObservedAt: start, Classification: sdk.DriftUnexplained}
	second := first
	second.Fields = []sdk.DriftField{{Path: first.Fields[0].Path, Old: "9999", New: "9000"}}
	queue.Add(first)
	queue.Add(second)
	if err := queue.DispatchReady(context.Background(), start.Add(19*time.Second)); err != nil || len(worker.requests) != 0 {
		t.Fatalf("dispatched before batch window: %v", err)
	}
	if err := queue.DispatchReady(context.Background(), start.Add(20*time.Second)); err != nil || len(worker.requests) != 1 || len(worker.requests[0].Events) != 1 {
		t.Fatalf("expected one deduplicated event: requests=%d, err=%v", len(worker.requests), err)
	}
	third := first
	third.Fields = []sdk.DriftField{{Path: first.Fields[0].Path, Old: "9000", New: "9001"}}
	third.ObservedAt = start.Add(21 * time.Second)
	queue.Add(third)
	if err := queue.DispatchReady(context.Background(), start.Add(41*time.Second)); err != nil || len(worker.requests) != 1 || queue.Pending() != 1 {
		t.Fatalf("over-budget event was dropped or dispatched: pending=%d requests=%d err=%v", queue.Pending(), len(worker.requests), err)
	}
}

func TestDriftInvestigationRetryUsesCanonicalRequest(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{}, err: context.DeadlineExceeded}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, DispatchesPerHour: 2})
	queue.Add(sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fields: []sdk.DriftField{{Path: "spec.port"}}, ObservedAt: start, Classification: sdk.DriftUnexplained})
	_ = queue.DispatchReady(context.Background(), start.Add(time.Second))
	first := queue.ExportState().RequestJSON
	if first == "" {
		t.Fatal("request was not persisted")
	}
	worker.err = nil
	restarted := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, DispatchesPerHour: 2})
	restarted.RestoreState(queue.ExportState())
	if err := restarted.DispatchReady(context.Background(), start.Add(2*time.Second)); err != nil || len(worker.requests) != 2 {
		t.Fatalf("retry changed canonical request: %v", err)
	}
	for _, request := range worker.requests {
		wire, err := canonicalProtoJSON(request)
		if err != nil || string(wire) != first {
			t.Fatalf("retry changed request: %v", err)
		}
	}
}

func TestDriftTokenBudgetLeavesNextEventPending(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{InputTokens: 9, OutputTokens: 1}}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, TokensPerHour: 10})
	first := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "api"}, Fields: []sdk.DriftField{{Path: "spec.port"}}, ObservedAt: start}
	queue.Add(first)
	if err := queue.DispatchReady(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	usage := queue.ExportState().TokenUsage
	if len(usage) != 1 || usage[0].InputTokens != 9 || usage[0].OutputTokens != 1 || usage[0].Tokens != 10 {
		t.Fatalf("drift usage omitted token breakdown: %+v", usage)
	}
	second := first
	second.Fields = []sdk.DriftField{{Path: "spec.port", New: "9001"}}
	second.ObservedAt = start.Add(2 * time.Second)
	queue.Add(second)
	if err := queue.DispatchReady(context.Background(), start.Add(3*time.Second)); err != nil || len(worker.requests) != 1 || queue.Pending() != 1 {
		t.Fatalf("token budget dispatched or lost pending event: requests=%d pending=%d err=%v", len(worker.requests), queue.Pending(), err)
	}
}

func TestFirstDriftKeyBypassesSpentTokenBudget(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{InputTokens: 55000, OutputTokens: 500}}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, TokensPerHour: 50000})
	jaeger := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "jaeger"}, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999"}}, ObservedAt: start}
	queue.Add(jaeger)
	if err := queue.DispatchReady(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	target := jaeger
	target.Ref.Name = "reservation"
	target.ObservedAt = start.Add(2 * time.Second)
	queue.Add(target)
	if err := queue.DispatchReady(context.Background(), start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 2 || worker.requests[1].Events[0].Ref.Name != "reservation" {
		t.Fatalf("new targetPort drift waited behind spent token budget: %+v", worker.requests)
	}
}

func TestInFlightDriftDuplicateIsNotQueued(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: &recordingDriftInvestigator{}, BatchWindow: time.Second})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "jaeger"}, Fields: []sdk.DriftField{{Path: "spec.port", New: "9999"}}, ObservedAt: start}
	queue.Add(event)
	task, err := queue.PrepareReady(context.Background(), start.Add(time.Second))
	if err != nil || task == nil {
		t.Fatalf("prepare: %v", err)
	}
	duplicate := event
	duplicate.Fields = []sdk.DriftField{{Path: "spec.port", Old: "8080", New: "9999"}}
	queue.Add(duplicate)
	if err := queue.Complete(start.Add(2*time.Second), task, &contracts.DriftInvestigationResult{}, nil); err != nil {
		t.Fatal(err)
	}
	if queue.Pending() != 0 {
		t.Fatalf("in-flight duplicate queued after completion: %+v", queue.ExportState())
	}
}

func TestDriftVerdictSurvivesTransientStateReset(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := NewConfigMapStateStore(client, "control", "sdo-controller-state")
	store.EnableDriftVerdicts()
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{Events: []*contracts.DriftEventInvestigation{{Verdict: "unsure", Reason: "pre-existing"}}}}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "jaeger"}, Fields: []sdk.DriftField{{Path: "spec.port", New: "9999"}}, ObservedAt: start}
	queue.Add(event)
	if err := queue.DispatchReady(ctx, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	state := RuntimeState{Version: RuntimeStateVersion, DriftInvestigation: queue.ExportState()}
	if _, err := store.Save(ctx, state, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().ConfigMaps("control").Delete(ctx, "sdo-controller-state", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	restarted := NewConfigMapStateStore(client, "control", "sdo-controller-state")
	restarted.EnableDriftVerdicts()
	loaded, _, err := restarted.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.DriftInvestigation.Verdicts) != 1 {
		t.Fatalf("reset discarded completed verdict: %+v", loaded.DriftInvestigation)
	}
}

func TestHermeticDriftVerdictAcrossFastloopResetAndNextIncident(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0).UTC()
	client := fake.NewSimpleClientset()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{
		Events:      []*contracts.DriftEventInvestigation{{Verdict: "unsure", Reason: "pre-existing"}},
		InputTokens: 55000, OutputTokens: 500,
	}}
	newController := func(at time.Time) *Controller {
		t.Helper()
		config := testControllerConfig()
		config.Namespace = "app"
		controller, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}},
			&recordingDispatcher{requests: make(chan IncidentRequest, 1)}, at)
		if err != nil {
			t.Fatal(err)
		}
		controller.ConfigureDrift(DriftControllerConfig{Events: cache, View: func(snapshot sdk.DetectionContext) program.ReadView {
			return driftReadView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}, diff: program.Diff{Changed: true}}
		}, Investigation: DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, TokensPerHour: 50000}})
		if err := controller.AttachStateStore(ctx, NewConfigMapStateStore(client, "control", "sdo-controller-state")); err != nil {
			t.Fatal(err)
		}
		return controller
	}
	controller := newController(start)
	jaegerBefore, jaegerDrift := driftService(8080), driftService(9999)
	jaegerBefore.Name, jaegerDrift.Name = "jaeger", "jaeger"
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(jaegerBefore, jaegerDrift)
	if err := controller.StepEvents(ctx, start, cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := controller.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 1 || controller.ExportState().DriftEvents[0].InvestigationVerdict != "unsure" {
		t.Fatalf("pre-existing drift was not investigated: %+v", controller.ExportState())
	}
	if err := client.CoreV1().ConfigMaps("control").Delete(ctx, "sdo-controller-state", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	controller = newController(start.Add(10 * time.Second))
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(jaegerBefore, jaegerDrift)
	if err := controller.StepEvents(ctx, start.Add(10*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	reservationBefore, reservationDrift := driftService(8080), driftService(9999)
	reservationBefore.Name, reservationDrift.Name = "reservation", "reservation"
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(reservationBefore, reservationDrift)
	if err := controller.StepEvents(ctx, start.Add(11*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := controller.StepEvents(ctx, start.Add(12*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 2 || worker.requests[1].Events[0].Ref.Name != "reservation" {
		t.Fatalf("second incident repeated jaeger or delayed new drift: %+v", worker.requests)
	}
	jaegerChanged := driftService(9000)
	jaegerChanged.Name = "jaeger"
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(jaegerDrift, jaegerChanged)
	if err := controller.StepEvents(ctx, start.Add(time.Hour+13*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if err := controller.StepEvents(ctx, start.Add(time.Hour+14*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 3 || worker.requests[2].Events[0].Ref.Name != "jaeger" {
		t.Fatalf("changed jaeger drift did not invalidate its verdict: %+v", worker.requests)
	}
}

func TestDriftReinvestigationBudgetDeferralHasTypedStatus(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	worker := &recordingDriftInvestigator{result: &contracts.DriftInvestigationResult{InputTokens: 55000, OutputTokens: 500}}
	queue := newDriftInvestigationQueue(DriftInvestigationConfig{Worker: worker, BatchWindow: time.Second, TokensPerHour: 50000})
	event := sdk.DriftEvent{Ref: sdk.ObjectRef{Kind: "Service", Name: "jaeger"}, Fields: []sdk.DriftField{{Path: "spec.port", New: "9999"}}, ObservedAt: start}
	queue.Add(event)
	if err := queue.DispatchReady(context.Background(), start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	event.Fields[0].New = "9000"
	event.ObservedAt = start.Add(2 * time.Second)
	queue.Add(event)
	if err := queue.DispatchReady(context.Background(), start.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	state := queue.ExportState()
	if len(worker.requests) != 1 || state.Deferral == nil || state.Deferral.Reason != DriftTokenBudget || !state.Deferral.Until.After(start.Add(3*time.Second)) {
		t.Fatalf("re-investigation budget deferral missing from status: %+v", state)
	}
	newKey := event
	newKey.Ref.Name = "reservation"
	newKey.ObservedAt = start.Add(4 * time.Second)
	queue.Add(newKey)
	if err := queue.DispatchReady(context.Background(), start.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(worker.requests) != 2 || len(worker.requests[1].Events) != 1 || worker.requests[1].Events[0].Ref.Name != "reservation" || queue.Pending() != 1 || queue.ExportState().Deferral != nil {
		t.Fatalf("new key waited behind deferred re-investigation: %+v", queue.ExportState())
	}
}
