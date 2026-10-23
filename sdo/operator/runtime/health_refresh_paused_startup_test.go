package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// These are the RV6b overlay probes at run.go's startup effect seam. A pause
// skips Step, so each restart must retire stale evidence before any effects.
func TestRV6PausedStartupRetiresStaleEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	root, _, _, _, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingSwap(t, root)
	current := tracingRefresh(t, root, coordinator)
	for _, phase := range []string{"open", "pending", "acknowledgment"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Unix(1000, 0)
			cfg := testControllerConfig()
			cfg.SourceCommit = current.Commit
			initial, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := initial.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
			request.SourceCommit = old.Commit
			request.DetectorHistory = testDetectorHistory(now)
			if phase == "open" {
				initial.currentIncidentRequest = &request
				initial.incidentOpen = true
				initial.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
			} else {
				initial.pendingClosure = &IncidentClosure{Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)), FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}}, DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now}
				initial.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
				if phase == "acknowledgment" {
					initial.closureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
					initial.closureReceipt = &ClosureReceipt{IncidentID: request.IncidentID, OutcomeCommit: old.Commit, AckToken: "obsolete"}
				}
			}
			store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo-sdo", "state")
			if _, err := store.Save(ctx, exportState(t, initial), ""); err != nil {
				t.Fatal(err)
			}
			visible := 0
			for restart := 1; restart <= 3; restart++ {
				detector := controllerDetector("health", time.Second)
				dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
				restored, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{}, dispatcher, now)
				if err != nil {
					t.Fatal(err)
				}
				if err := restored.AttachStateStore(ctx, store); err != nil {
					t.Fatal(err)
				}
				broker := &recordingIncidentBroker{}
				if err := restored.SetIncidentBroker(broker); err != nil {
					t.Fatal(err)
				}
				restored.discardRestoredHealthObservations(now)
				rv6Guard(restored, root, current.Commit)
				restored.OnClosureFailed = func(f ClosureFailure) {
					if f.Permanent && strings.Contains(f.LastError, "fresh investigation") {
						visible++
					}
				}
				// Match startup: persistence failure is logged, then startup effects run
				// before the maintenance watcher is installed. Do not call Step here.
				if err := restored.PersistState(ctx); err != nil {
					t.Logf("startup persistence: %v", err)
				}
				err = executePendingEffects(ctx, restored)
				t.Logf("restart=%d startup=%v phase=%s failures=%d", restart, err, restored.closureState, len(restored.failedClosures))
				if err != nil {
					t.Errorf("paused startup cannot reach maintenance watcher: %v", err)
				}
				if err := executePausedEffects(ctx, restored); err != nil {
					t.Errorf("paused effects: %v", err)
				}
				if detector.calls != 0 || len(dispatcher.requests) != 0 || len(broker.calls()) != 0 {
					t.Fatal("paused recovery evaluated detectors or executed obsolete effects")
				}
				saved, _, err := store.Load(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if saved.GetIncidentOpen() || saved.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_FAILED || len(saved.GetFailedClosures()) != 1 {
					t.Errorf("restart %d lacks durable terminal failure: open=%v phase=%s failures=%d", restart, saved.GetIncidentOpen(), saved.GetClosureState(), len(saved.GetFailedClosures()))
				}
				if failure := saved.GetClosureFailure(); failure == nil || !failure.GetPermanent() || !strings.Contains(failure.GetLastError(), "fresh investigation") {
					t.Errorf("restart %d lacks visible stale-evidence failure: %v", restart, failure)
				}
			}
			if visible != 1 {
				t.Errorf("want one failure notification across restarts, got %d", visible)
			}
		})
	}
}
