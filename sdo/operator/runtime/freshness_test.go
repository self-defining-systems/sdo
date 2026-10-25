package runtime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func freshnessSnapshot() sdktest.Snapshot {
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	return sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: sdk.ScopedSnapshot{
		Version: 1, Generation: "same-generation",
		Scope:    sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}},
		Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}},
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "dependency", Namespace: "foreign"}}},
	}}
}

func TestRV10bRestoredClosureAfterSameGenerationDenial(t *testing.T) {
	for _, paused := range []bool{false, true} {
		name := "normal"
		if paused {
			name = "paused"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			start := time.Unix(100, 0)
			config := testControllerConfig()
			config.Namespace, config.FiringThreshold, config.ClearThreshold = "app", 1, 1
			config.VerificationTimeout = 30 * time.Second
			snapshot := freshnessSnapshot()
			detector := controllerDetector("fault", time.Second, stateFinding("fault"), sdk.Finding{})
			detector.spec.Watches = []sdk.WatchKind{{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}}
			create := func(s sdktest.Snapshot) *Controller {
				c, err := NewController(config, []sdk.Detector{detector}, staticProvider{snapshot: s}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
				if err != nil {
					t.Fatal(err)
				}
				c.now = func() time.Time { return start.Add(2 * time.Second) }
				return c
			}
			first := create(snapshot)
			if err := first.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			first.responderDone, first.responderCompletedAt = true, start
			if err := first.SetIncidentBroker(&recordingIncidentBroker{}); err != nil {
				t.Fatal(err)
			}
			if err := first.Step(ctx, start.Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			state := exportState(t, first)
			if state.PendingClosure == nil || len(state.PendingClosure.FinalDetectorStates) == 0 {
				t.Fatal("fixture needs a verified closure")
			}
			completed := state.PendingClosure.ResponderCompletedAt.AsTime()
			snapshot.ScopedSnapshot.Statuses[0].State = "forbidden"
			snapshot.ScopedSnapshot.Services = nil
			restored := create(snapshot)
			if err := restored.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			broker := &recordingIncidentBroker{}
			if err := restored.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			if err := executeStartupEffects(ctx, restored, paused); err != nil {
				t.Fatal(err)
			}
			after := exportState(t, restored)
			restored.mu.Lock()
			processing := restored.closureState == contracts.ClosureState_CLOSURE_STATE_PROCESSING
			restored.mu.Unlock()
			if processing {
				awaitBrokerResult(t, restored.closureResults)
			}
			if calls := broker.calls(); len(calls) != 0 {
				t.Errorf("stale verified closure reached broker: %v", calls)
			}
			if after.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED || after.ClosureFailure == nil || !strings.Contains(after.ClosureFailure.LastError, "observation") {
				t.Errorf("denied closure was not visibly retired: state=%s failure=%v", after.ClosureState, after.ClosureFailure)
			}
			if !after.PendingClosure.ResponderCompletedAt.AsTime().Equal(completed) {
				t.Fatal("restart reset the original verification timeout")
			}
			if len(after.FailedClosures) != 1 {
				t.Errorf("retirement archive count=%d", len(after.FailedClosures))
			}
		})
	}
}

func TestRV10bFreshDiagnosisAfterSameGenerationDenial(t *testing.T) {
	root := t.TempDir()
	snapshot := freshnessSnapshot()
	request := IncidentRequest{IncidentID: "fresh-diagnosis", Namespace: "app", ObservationGeneration: snapshot.ObservationGeneration()}
	if err := memory.CaptureIncidentSnapshot(root, request.IncidentID, "open@detect", snapshot, "app"); err != nil {
		t.Fatal(err)
	}
	dir, err := memory.IncidentCorpusDir(root, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "open@detect.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ScopedSnapshot.Statuses[0].State = "forbidden"
	snapshot.ScopedSnapshot.Services = nil
	provider := staticProvider{snapshot: snapshot}
	// This is the operational view used by InitialDiagnose, including after restart.
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 1)
	loop.CorpusSessionRoot, loop.capturePhase = root, "open@detect"
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s} }
	view, err := loop.snapshotView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	services, err := view.(sdk.ScopedObservationSource).ScopedServices("foreign")
	if err == nil {
		t.Errorf("fresh diagnosis returned historical availability: objects=%d", len(services))
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("fresh diagnosis rewrote immutable historical open@detect")
	}
}

func TestClosureHandoffRechecksDetectorState(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "active"
		if missing {
			name = "missing"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(100, 0)
			broker := &recordingIncidentBroker{}
			c := newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)
			// The frozen success's detector either starts firing or disappears.
			if missing {
				delete(c.detectors, "health")
			} else {
				c.detectors["health"] = controllerDetector("health", time.Second, stateFinding("health"))
			}
			effect, ok := c.PendingClosureEffect()
			if !ok {
				t.Fatal("missing pending closure")
			}
			if err := c.ExecuteClosureEffect(context.Background(), effect); err != nil {
				t.Fatal(err)
			}
			state := exportState(t, c)
			if len(broker.calls()) != 0 || state.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED || state.ClosureFailure == nil {
				t.Fatalf("obsolete detector success remained eligible: state=%v failure=%v calls=%v", state.ClosureState, state.ClosureFailure, broker.calls())
			}
		})
	}
}

func TestClosureHandoffRechecksSourceFieldsAfterVerification(t *testing.T) {
	f := newClosureDriftFixture(t, true, true, "app")
	f.restoreSource(t)
	f.step(t, 1)
	if len(f.closed) != 1 || closureOutcome(t, f.closed[0]) != "success" {
		t.Fatal("fixture needs verified image and source repair")
	}
	service, err := f.client.CoreV1().Services("app").Get(context.Background(), "reservation", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort.IntVal = 9999
	if _, err := f.client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	broker := &recordingIncidentBroker{}
	if err := f.c.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	effect, ok := f.c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing pending closure")
	}
	if err := f.c.ExecuteClosureEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, f.c)
	if len(broker.calls()) != 0 || state.ClosureState != contracts.ClosureState_CLOSURE_STATE_FAILED || state.ClosureFailure == nil || !strings.Contains(state.ClosureFailure.LastError, "source fields") {
		t.Fatalf("source regression reused old verification: state=%v failure=%v calls=%v", state.ClosureState, state.ClosureFailure, broker.calls())
	}
}



func TestUnverifiedClosureRejectsStaleEffectIdentity(t *testing.T) {
	now := time.Unix(100, 0)
	broker := &recordingIncidentBroker{}
	c := newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)
	delete(c.detectors, "health")
	effect, ok := c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing pending closure")
	}
	effect.Identity = "stale"
	if err := c.ExecuteClosureEffect(context.Background(), effect); err == nil {
		t.Fatal("stale effect retired pending work")
	}
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("stale effect consumed pending closure")
	}
}
