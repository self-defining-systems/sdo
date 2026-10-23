package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCloneIncidentResultPreservesEmptyCollectionsForBrokerProtocol(t *testing.T) {
	result := completedResult("incident-empty-collections")
	result.AppliedPlaybooks = []AppliedPlaybook{{PlaybookID: "health-objective", ScriptNames: []string{}}}
	result.FinalDetectorStates = []DetectorEvaluation{}
	result.ProposedArtifactIDs = []string{}
	result.RepairActions = []RepairActionReceipt{}

	typed, err := incidentResultContract(*cloneIncidentResult(&result))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.Canonical(typed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(":null")) {
		t.Fatalf("canonical result emitted null: %s", payload)
	}
	var decoded contracts.IncidentResult
	if err := wire.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.GetFinalDetectorStates()) != 0 || len(decoded.GetAppliedPlaybooks()[0].GetScriptNames()) != 0 {
		t.Fatal("empty collections changed on ProtoJSON round trip")
	}
}

func TestStateRoundTripRetainsAppliedActionsAndProgramEvidenceWithoutRollbackState(t *testing.T) {
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "api"}
	before := planObject(ref, map[string]any{"replicas": float64(1)})
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	action := program.SetField{Ref: ref, Path: "spec.replicas", Value: testPlanValue(2), Reason: "restore capacity"}
	application, err := executor.Apply(ctx, IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
	if err != nil {
		t.Fatal(err)
	}
	trust := map[string]ProgramTrust{"capacity": {VerifiedSuccesses: 3, LastChange: "abc", Trusted: true}}
	response, err := json.Marshal(ToolResponse{Command: "status", Trust: trust, ProgramRuns: []ProgramRun{{ProgramID: "capacity", Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}, Verdict: program.Verdict{Result: program.NoMatch}}}})
	if err != nil {
		t.Fatal(err)
	}
	state := &contracts.ControllerState{Version: RuntimeStateVersion, IncidentResult: mustProject(t, incidentResultContract, IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: "program-state", Status: IncidentCompleted, Timing: TimingMetrics{StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)},
		ProgramRuns:      []ProgramRun{{ProgramID: "capacity", Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}, Verdict: program.Verdict{Result: program.NoMatch}}},
		ProgramToolCalls: []ToolCall{{Command: "status", RequestJSON: `{"command":"status"}`, ResponseJSON: string(response), Summary: "healthy"}},
		PlanApplications: []RecordedPlanApplication{{ID: "application-1", ProgramIDs: []string{"capacity"}, Result: application}},
	})}
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
	if _, err := store.Save(ctx, state, ""); err != nil {
		t.Fatalf("persist applied plan: %v", err)
	}
	loaded, _, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("reload applied plan: %v", err)
	}
	result := incidentResultState(loaded.IncidentResult)
	if len(result.ProgramRuns) != 1 || !reflect.DeepEqual(result.ProgramRuns[0].Bindings, incidentResultState(state.IncidentResult).ProgramRuns[0].Bindings) ||
		!reflect.DeepEqual(result.ProgramToolCalls, incidentResultState(state.IncidentResult).ProgramToolCalls) {
		t.Fatalf("program evidence changed after reload: %+v", result)
	}
	var restoredResponse ToolResponse
	if err := json.Unmarshal([]byte(result.ProgramToolCalls[0].ResponseJSON), &restoredResponse); err != nil || !reflect.DeepEqual(restoredResponse.Trust, trust) {
		t.Fatalf("trust stats after reload: %+v, %v", restoredResponse.Trust, err)
	}
	got := result.PlanApplications[0].Result
	if len(got.PreImages) != 0 || len(got.Patches) != 0 || len(got.ActionsApplied) != 1 {
		t.Fatalf("outcome lost action evidence or retained rollback state: %+v", got)
	}
	if _, ok := got.ActionsApplied[0].(program.SetField); !ok {
		t.Fatalf("applied action type = %T", got.ActionsApplied[0])
	}
	if err := executor.Rollback(ctx, got); err == nil {
		t.Fatal("durable outcome authorized rollback")
	}
	if value, _ := testField(client.objects[ref], "spec.replicas"); value != float64(2) {
		t.Fatal("durable outcome undid fix")
	}

}

// A clean closing view has no changes. The broker's schema requires a list,
// so a persisted closure must say "changes":[] and never "changes":null,
// or every broker attempt fails and the closure fails permanently.
func TestCloneIncidentClosureKeepsAnEmptyClosingViewDiffAList(t *testing.T) {
	closure := nativeClosure(IncidentWorkspace{IncidentID: "empty-diff", Worktree: "/proposal", BaseCommit: "base"})
	closure.FinalStateChanges = &StateChanges{BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(1, 0).UTC(), Changes: []StateChange{}}
	typed, err := incidentClosureContract(*cloneIncidentClosure(&closure))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.Canonical(typed)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(":null")) {
		t.Fatalf("canonical closure emitted null: %s", payload)
	}
	var decoded contracts.IncidentClosure
	if err := wire.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetFinalStateChanges() == nil || len(decoded.GetFinalStateChanges().GetChanges()) != 0 {
		t.Fatal("closing diff lost its empty logical list")
	}
}

func TestConfigMapStateStoreRoundTripAndCASConflict(t *testing.T) {
	ctx := context.Background()
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "sdo-controller-state")
	state := &contracts.ControllerState{Version: RuntimeStateVersion, History: evaluationsContract([]DetectorEvaluation{{DetectorID: "health", EvaluatedAt: time.Unix(1, 0).UTC(), Status: DetectorEvaluationFiring}})}

	revision, err := store.Save(ctx, state, "")
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	loaded, loadedRevision, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loadedRevision != revision || len(loaded.History) != 1 {
		t.Fatalf("unexpected loaded state %#v revision %q", loaded, loadedRevision)
	}
	changed := loaded
	changed.History = append(changed.History, evaluationsContract([]DetectorEvaluation{{DetectorID: "incident", EvaluatedAt: time.Unix(2, 0).UTC(), Status: DetectorEvaluationFiring}})[0])
	if _, err := store.Save(ctx, changed, revision); err != nil {
		t.Fatalf("update state: %v", err)
	}
	if _, err := store.Save(ctx, changed, revision); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected stale writer conflict, got %v", err)
	}
}

func TestConfigMapStateStoreRejectsCorruptAndUnsupportedState(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{name: "corrupt", data: "not-json"},
		{name: "unsupported", data: `{"version":"sdo.controller/v999"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "state", Namespace: "demo", Annotations: map[string]string{stateRevisionAnnotation: "1"}},
				Data:       map[string]string{stateDataKey: test.data},
			})
			store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
			if _, _, err := store.Load(context.Background()); err == nil {
				t.Fatal("expected invalid state error")
			}
		})
	}
}

func TestControllerRestoreContinuesFiringCount(t *testing.T) {
	start := time.Unix(0, 0)
	detector := controllerDetector("fault", time.Second, stateFinding("fault"), stateFinding("fault"))
	firstDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	first, err := NewController(testControllerConfig(), []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, firstDispatcher, start)
	if err != nil {
		t.Fatalf("new first controller: %v", err)
	}
	if err := first.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("first sample: %v", err)
	}
	state := exportState(t, first)

	restoredDetector := controllerDetector("fault", time.Second, stateFinding("fault"))
	restoredDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(testControllerConfig(), []sdk.Detector{restoredDetector}, staticProvider{snapshot: sdktest.Snapshot{}}, restoredDispatcher, start)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if err := restored.Step(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatalf("second sample: %v", err)
	}
	executePendingEffect(t, restored)
	awaitRequest(t, restoredDispatcher.requests)
}

func TestControllerRestorePreservesIncidentLockWithoutRedispatch(t *testing.T) {
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	detector := controllerDetector("fault", time.Second, stateFinding("fault"))
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	first, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, start)
	if err != nil {
		t.Fatalf("new first controller: %v", err)
	}
	if err := first.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("dispatch sample: %v", err)
	}
	executePendingEffect(t, first)
	awaitRequest(t, dispatcher.requests)
	state := exportState(t, first)

	restoredDetector := controllerDetector("fault", time.Second, stateFinding("fault"), stateFinding("fault"))
	restoredDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(config, []sdk.Detector{restoredDetector}, staticProvider{snapshot: sdktest.Snapshot{}}, restoredDispatcher, start)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if err := restored.Step(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatalf("restored sample: %v", err)
	}
	assertNoRequest(t, restoredDispatcher.requests)
	if !restored.IncidentOpen() {
		t.Fatal("restore lost incident lock")
	}
}

func TestControllerRestoreResumesPendingDebounceBatch(t *testing.T) {
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.BatchDebounce = time.Second
	detector := controllerDetector("fault", time.Minute, stateFinding("fault"))
	first, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatalf("new first controller: %v", err)
	}
	if err := first.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("pending sample: %v", err)
	}

	restoredDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Minute, sdk.Finding{})}, staticProvider{snapshot: sdktest.Snapshot{}}, restoredDispatcher, start)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(exportState(t, first)); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if err := restored.Step(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatalf("debounce deadline: %v", err)
	}
	executePendingEffect(t, restored)
	awaitRequest(t, restoredDispatcher.requests)
}

func TestControllerRestoreDropsPendingFindingThatClearsBeforeDeadline(t *testing.T) {
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Second
	first, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", 500*time.Millisecond, stateFinding("transient"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		&recordingDispatcher{requests: make(chan IncidentRequest, 1)},
		start,
	)
	if err != nil {
		t.Fatalf("new first controller: %v", err)
	}
	if err := first.Step(context.Background(), start, nil); err != nil {
		t.Fatalf("firing sample: %v", err)
	}

	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", 500*time.Millisecond, sdk.Finding{})},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		start,
	)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.RestoreState(exportState(t, first)); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if err := restored.Step(context.Background(), start.Add(500*time.Millisecond), nil); err != nil {
		t.Fatalf("clear after restore: %v", err)
	}
	if err := restored.Step(context.Background(), start.Add(time.Second), nil); err != nil {
		t.Fatalf("debounce deadline after restore: %v", err)
	}
	if _, ok := restored.PendingDispatchEffect(); ok {
		t.Fatal("restored cleared finding survived pending debounce")
	}
	assertNoRequest(t, dispatcher.requests)
}

func TestDispatchEffectIsAbsentBeforeSaveAndReplayedAfterSave(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	controller, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("attach state store: %v", err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatalf("produce effect: %v", err)
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected deterministic pending effect")
	}

	beforeSave, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatalf("new pre-save restart: %v", err)
	}
	if err := beforeSave.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("attach empty store: %v", err)
	}
	if _, ok := beforeSave.PendingDispatchEffect(); ok {
		t.Fatal("restart before save recovered an effect that was never durable")
	}

	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist pending effect: %v", err)
	}
	afterSave, err := NewController(config, []sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))}, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatalf("new post-save restart: %v", err)
	}
	if err := afterSave.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("restore saved effect: %v", err)
	}
	replayed, ok := afterSave.PendingDispatchEffect()
	if !ok || replayed.Request.IncidentID != effect.Request.IncidentID {
		t.Fatalf("saved effect was not replayed identically: got %#v want %#v", replayed, effect)
	}
}

func TestLeadershipGuardBlocksPersistenceAndDispatch(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	client := fake.NewSimpleClientset()
	elector := NewLeaseElector(client, "demo", "sdo", "leader", 10*time.Second)
	now := time.Now().UTC()
	elector.now = func() time.Time { return now }
	if leader, acquireErr := elector.TryAcquireOrRenew(ctx); acquireErr != nil || !leader {
		t.Fatalf("acquire leadership: leader=%v err=%v", leader, acquireErr)
	}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(
		config,
		[]sdk.Detector{controllerDetector("fault", time.Second, stateFinding("fault"))},
		staticProvider{snapshot: sdktest.Snapshot{}},
		dispatcher,
		start,
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo", "state")); err != nil {
		t.Fatalf("attach state store: %v", err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatalf("produce effect: %v", err)
	}
	controller.GuardAction = elector.GuardContext
	now = now.Add(11 * time.Second)
	if err := controller.PersistState(ctx); err == nil {
		t.Fatal("stale leader persisted state")
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected pending dispatch effect")
	}
	if err := controller.ExecuteDispatchEffect(ctx, effect); err == nil {
		t.Fatal("stale leader executed dispatch effect")
	}
	assertNoRequest(t, dispatcher.requests)
}

func TestFindingArrivingDuringIncidentIsDeferredAndRestartRecoverable(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(0, 0)
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	primary := controllerDetector("primary", time.Second, stateFinding("primary-fault"), sdk.Finding{})
	secondary := controllerDetector("secondary", time.Second, sdk.Finding{}, stateFinding("secondary-fault"))
	firstDispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 2), release: make(chan struct{}, 1)}
	t.Cleanup(func() { close(firstDispatcher.release) })
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	controller, err := NewController(config, []sdk.Detector{primary, secondary}, staticProvider{snapshot: sdktest.Snapshot{}}, firstDispatcher, start)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	if err := controller.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("attach state store: %v", err)
	}
	if err := controller.Step(ctx, start, nil); err != nil {
		t.Fatalf("primary firing: %v", err)
	}
	firstEffect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("expected primary dispatch effect")
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist primary effect: %v", err)
	}
	if err := controller.ExecuteDispatchEffect(ctx, firstEffect); err != nil {
		t.Fatalf("execute primary effect: %v", err)
	}
	awaitRequest(t, firstDispatcher.requests)
	if err := controller.Step(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatalf("secondary firing during response: %v", err)
	}
	if effect, ok := controller.PendingDispatchEffect(); ok {
		t.Fatalf("new fault dispatched while primary responder is running: %#v", effect)
	}
	state := exportState(t, controller)
	if !state.IncidentOpen || state.IncidentRequest == nil || state.IncidentRequest.IncidentId != firstEffect.Request.IncidentID {
		t.Fatalf("new fault replaced the running primary incident: %#v", state.IncidentRequest)
	}
	if len(state.PendingBatch.Findings) != 1 || state.PendingBatch.Findings[0].Fingerprint != "secondary-fault" {
		t.Fatalf("new fault was not buffered exactly once: %#v", state.PendingBatch.Findings)
	}

	// Receiving the request does not imply that Dispatch has returned or that
	// its completion is queued. Hold the responder through the late finding,
	// then synchronize on its queued completion before the next controller step.
	firstDispatcher.release <- struct{}{}
	select {
	case completion := <-controller.results:
		controller.results <- completion
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for primary responder completion")
	}
	if err := controller.Step(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatalf("dispatch buffered secondary fault after primary completion: %v", err)
	}
	deferredEffect, ok := controller.PendingDispatchEffect()
	if !ok || len(deferredEffect.Request.Findings) != 1 || deferredEffect.Request.Findings[0].Fingerprint != "secondary-fault" {
		t.Fatalf("new fault was not deferred exactly once: %#v", deferredEffect)
	}
	if deferredEffect.Request.IncidentID == firstEffect.Request.IncidentID {
		t.Fatal("deferred fault reused the primary incident ID")
	}
	if err := controller.PersistState(ctx); err != nil {
		t.Fatalf("persist deferred effect: %v", err)
	}

	restoredDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	restored, err := NewController(config, []sdk.Detector{
		controllerDetector("primary", time.Second, sdk.Finding{}),
		controllerDetector("secondary", time.Second, stateFinding("secondary-fault")),
	}, staticProvider{snapshot: sdktest.Snapshot{}}, restoredDispatcher, start)
	if err != nil {
		t.Fatalf("new restored controller: %v", err)
	}
	if err := restored.AttachStateStore(ctx, store); err != nil {
		t.Fatalf("restore deferred effect: %v", err)
	}
	replayed, ok := restored.PendingDispatchEffect()
	if !ok || replayed.Request.IncidentID != deferredEffect.Request.IncidentID {
		t.Fatalf("deferred effect changed across restart: %#v", replayed)
	}
	if err := restored.ExecuteDispatchEffect(ctx, replayed); err != nil {
		t.Fatalf("execute deferred effect: %v", err)
	}
	request := awaitRequest(t, restoredDispatcher.requests)
	if len(request.Findings) != 1 || request.Findings[0].Fingerprint != "secondary-fault" {
		t.Fatalf("deferred finding was not delivered once: %#v", request.Findings)
	}
}

func TestControllerStateDropsRetiredRollbackFieldsFromPriorBuild(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	action := program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080), Reason: "restore port"}
	result := completedResult("legacy-state")
	result.PlanApplications = []RecordedPlanApplication{{ID: "old-application", ProgramIDs: []string{"targetport"}, Result: PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, Applied: true, ActionsApplied: []program.Action{action}}}}
	result.ProgramFailureSignals = []string{"fast_path_failure:targetport@original-revision"}
	payload, err := encodeControllerState(&contracts.ControllerState{Version: RuntimeStateVersion, IncidentResult: mustProject(t, incidentResultContract, result)})
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	application := document["incident_result"].(map[string]any)["plan_applications"].([]any)[0].(map[string]any)
	application["pre_images"] = []any{map[string]any{"ref": map[string]any{"name": "retired"}, "object": map[string]any{"spec": map[string]any{"replicas": 9}}}}
	application["patches"] = []any{map[string]any{"patch_json": "retired mutation"}}
	payload, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := decodeControllerState(payload)
	if err != nil {
		t.Fatal(err)
	}
	got := incidentResultState(loaded.IncidentResult).PlanApplications[0].Result
	if len(got.PreImages) != 0 || len(got.Patches) != 0 || len(got.ActionsApplied) != 1 || !got.Applied {
		t.Fatalf("retired fields corrupted outcome facts: %+v", got)
	}
	if !reflect.DeepEqual(loaded.IncidentResult.ProgramFailureSignals, result.ProgramFailureSignals) {
		t.Fatal("lost revision-bound failure")
	}
}
