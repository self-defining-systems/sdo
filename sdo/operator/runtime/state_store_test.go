package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCloneIncidentResultPreservesEmptyCollectionsForBrokerProtocol(t *testing.T) {
	result := completedResult("incident-empty-collections")
	result.AppliedPlaybooks = []AppliedPlaybook{{PlaybookID: "health-objective", ScriptNames: []string{}}}
	result.FinalDetectorStates = []DetectorEvaluation{}
	result.ProposedArtifactIDs = []string{}
	result.RepairActions = []RepairActionReceipt{}

	payload, err := json.Marshal(cloneIncidentResult(&result))
	if err != nil {
		t.Fatalf("marshal cloned result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode cloned result: %v", err)
	}
	if decoded["final_detector_states"] == nil || decoded["proposed_artifact_ids"] == nil ||
		decoded["repair_actions"] == nil {
		t.Fatalf("empty result collections became null: %s", payload)
	}
	applied := decoded["applied_playbooks"].([]any)[0].(map[string]any)
	if applied["script_names"] == nil {
		t.Fatalf("empty applied playbook scripts became null: %s", payload)
	}
}

// A clean closing view has no changes. The broker's schema requires a list,
// so a persisted closure must say "changes":[] and never "changes":null,
// or every broker attempt fails and the closure fails permanently.
func TestCloneIncidentClosureKeepsAnEmptyClosingViewDiffAList(t *testing.T) {
	closure := IncidentClosure{
		FinalStateChanges: &StateChanges{BaselineAt: time.Unix(0, 0).UTC(), ObservedAt: time.Unix(1, 0).UTC(), Changes: []StateChange{}},
	}
	encoded, err := json.Marshal(cloneIncidentClosure(&closure))
	if err != nil {
		t.Fatalf("marshal closure: %v", err)
	}
	var decoded struct {
		FinalStateChanges map[string]json.RawMessage `json:"final_state_changes"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal closure: %v", err)
	}
	if got := string(decoded.FinalStateChanges["changes"]); got != "[]" {
		t.Fatalf("an empty closing-view diff must encode as [], got %s", got)
	}
	view := cloneIncidentView(&IncidentView{StateChanges: closure.FinalStateChanges})
	if view.StateChanges.Changes == nil {
		t.Fatal("an empty live-view diff must stay a list, not null")
	}
}

func TestConfigMapStateStoreRoundTripAndCASConflict(t *testing.T) {
	ctx := context.Background()
	store := NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "sdo-controller-state")
	state := RuntimeState{Version: RuntimeStateVersion, History: []DetectorEvaluation{{DetectorID: "health"}}}

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
	changed.History = append(changed.History, DetectorEvaluation{DetectorID: "incident"})
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
			store := NewConfigMapStateStore(client, "demo", "state")
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
	state := first.ExportState()

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
	state := first.ExportState()

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
	if err := restored.RestoreState(first.ExportState()); err != nil {
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
	if err := restored.RestoreState(first.ExportState()); err != nil {
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
	store := NewConfigMapStateStore(client, "demo", "state")
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
	if err := controller.AttachStateStore(ctx, NewConfigMapStateStore(client, "demo", "state")); err != nil {
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
	firstDispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 2)}
	client := fake.NewSimpleClientset()
	store := NewConfigMapStateStore(client, "demo", "state")
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
	deferredEffect, ok := controller.PendingDispatchEffect()
	if !ok || deferredEffect.Request.Findings[0].Fingerprint != "secondary-fault" {
		t.Fatalf("new fault was not deferred exactly once: %#v", deferredEffect)
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
