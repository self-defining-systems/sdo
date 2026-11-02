package runtime

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// STALE3: seeded crash cuts preserve a verified S1 closure and change the
// observation scope to S2 before/after restore or effect selection. The
// detector scope changing is not evidence about the cluster: the closure is
// rechecked under the scope now in force and, being healthy, reaches the broker.
func TestScopedClosureVerifiedUnderChangedScope(t *testing.T) {
	for seed := int64(0); seed < 16; seed++ {
		for _, cut := range []string{"scope-before-restore", "scope-after-restore", "scope-after-selection", "unchanged"} {
			t.Run(fmt.Sprintf("%d/%s", seed, cut), func(t *testing.T) {
				start := time.Unix(100, 0)
				scope := sdk.ScopedSnapshot{Version: 1, Generation: "S1", Scope: sdk.ObservationScope{Version: 1}}
				initialProvider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scope}}
				// Open a real incident against the S1 snapshot.
				config := testControllerConfig()
				config.Namespace, config.FiringThreshold, config.ClearThreshold = "app", 1, 1
				detector := controllerDetector("fault", time.Second, stateFinding("fault"), sdk.Finding{})
				first, err := NewController(config, []sdk.Detector{detector}, initialProvider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
				if err != nil {
					t.Fatal(err)
				}
				if err = first.Step(context.Background(), start, nil); err != nil {
					t.Fatal(err)
				}
				first.responderDone = true
				first.responderCompletedAt = start
				// Clear the incident detector on the next evaluation.
				broker := &recordingIncidentBroker{}
				if err = first.SetIncidentBroker(broker); err != nil {
					t.Fatal(err)
				}
				for _, at := range []time.Duration{time.Second, time.Second + first.config.ClosureStabilityWindow} {
					if err = first.Step(context.Background(), start.Add(at), nil); err != nil {
						t.Fatal(err)
					}
				}
				state := exportState(t, first)
				if state.PendingClosure == nil || len(state.PendingClosure.FinalDetectorStates) == 0 {
					t.Fatal("fixture needs verified S1 success")
				}
				rng := rand.New(rand.NewSource(seed))
				scope.Generation = fmt.Sprintf("S2-%d", rng.Intn(10000))
				scope.Scope.Targets = []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "new-required"}}
				scope.Statuses = []sdk.ObservationStatus{{Target: scope.Scope.Targets[0], State: "forbidden"}}
				oldProvider := first.provider
				changedProvider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: scope}}
				provider := oldProvider
				if cut == "scope-before-restore" {
					provider = changedProvider
				}
				restored, err := NewController(config, []sdk.Detector{detector}, provider, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
				if err != nil {
					t.Fatal(err)
				}
				if err = restored.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				if err = restored.SetIncidentBroker(broker); err != nil {
					t.Fatal(err)
				}
				if cut == "scope-after-restore" {
					restored.provider = changedProvider
				}
				effect, ok := restored.PendingClosureEffect()
				if !ok {
					t.Fatal("missing restored closure")
				}
				if cut == "scope-after-selection" {
					restored.provider = changedProvider
				}
				err = restored.ExecuteClosureEffect(context.Background(), effect)
				if err != nil {
					t.Fatalf("closure under %s refused: %v", cut, err)
				}
				awaitBrokerResult(t, restored.closureResults)
				if len(broker.calls()) != 1 {
					t.Fatalf("closure under %s did not reach the broker", cut)
				}
				if restored.closureFailure != nil {
					t.Fatalf("closure under %s retired: %v", cut, restored.closureFailure)
				}
			})
		}
	}
}

// STALE3: a diagnosis keeps running when the scope changes under it.
func TestScopedDiagnosisSurvivesScopeChange(t *testing.T) {
	root := t.TempDir()
	request := IncidentRequest{IncidentID: "generation", Namespace: "app", ObservationGeneration: "S1"}
	phase := "open@detect"
	snapshot := sdktest.Snapshot{NamespaceName: "app", ScopedSnapshot: sdk.ScopedSnapshot{Version: 1, Generation: "S1"}}
	loop := ResponderLoop{Request: request, CorpusSessionRoot: root, capturePhase: phase, Provider: staticProvider{snapshot: snapshot}, ReadView: func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s} }}
	if _, err := loop.snapshotView(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot.ScopedSnapshot.Generation = "S2"
	loop.Provider = staticProvider{snapshot: snapshot}
	if _, err := loop.snapshotView(context.Background()); err != nil {
		t.Fatalf("scope change failed the diagnosis: %v", err)
	}
}
