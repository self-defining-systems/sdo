package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// These are the RV6b overlay probes at run.go's startup effect seam. A pause
// skips Step, so each restart must retire stale evidence before any effects.
func TestRV6PausedStartupRetiresStaleEvidence(t *testing.T) {
	for _, change := range []string{"source", "scope"} {
		t.Run(change, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			root, _, _, _, coordinator := tracingFixture(t)
			old := tracingRefresh(t, root, coordinator)
			if change == "source" {
				tracingSwap(t, root)
			} else {
				// Narrow the accepted health objective without changing application
				// source: prior successful evidence cannot authorize the new scope.
				if err := memory.WriteGoal(root, "tracing", "Keep tracing healthy, including collector availability."); err != nil {
					t.Fatal(err)
				}
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "change health scope")
			}
			current := tracingRefresh(t, root, coordinator)
			for _, entry := range []struct {
				name string
				run  func(context.Context, *Controller) error
			}{
				{"startup", func(ctx context.Context, c *Controller) error { return executeStartupEffects(ctx, c, true) }},
				{"paused", executePausedEffects},
				{"pending", executePendingEffects},
			} {
				for _, phase := range []string{"open", "pending", "acknowledgment"} {
					t.Run(entry.name+"/"+phase, func(t *testing.T) {
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
						client := fake.NewSimpleClientset()
						store := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "state")
						if _, err := store.Save(ctx, exportState(t, initial), ""); err != nil {
							t.Fatal(err)
						}
						visible := 0
						for restart := 1; restart <= 3; restart++ {
							detector := controllerDetector("health", time.Second, sdk.Finding{})
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
							err = entry.run(ctx, restored)
							t.Logf("restart=%d startup=%v phase=%s failures=%d", restart, err, restored.closureState, len(restored.failedClosures))
							if err != nil {
								t.Errorf("paused startup cannot reach maintenance watcher: %v", err)
							}
							if err := executeStartupEffects(ctx, restored, true); err != nil {
								t.Errorf("paused effects: %v", err)
							}
							if detector.calls != 0 || len(dispatcher.requests) != 0 || len(broker.calls()) != 0 {
								t.Fatal("paused recovery evaluated detectors or executed obsolete effects")
							}
							saved, _, err := store.Load(ctx)
							if err != nil {
								t.Fatal(err)
							}
							if saved.GetIncidentOpen() || saved.GetClosureState() != contracts.ClosureState_CLOSURE_STATE_FAILED {
								t.Errorf("restart %d lacks durable terminal failure: open=%v phase=%s failures=%d", restart, saved.GetIncidentOpen(), saved.GetClosureState(), len(saved.GetFailedClosures()))
							}
							archives, err := client.CoreV1().ConfigMaps("demo-sdo").List(ctx, metav1.ListOptions{LabelSelector: "sdo.dev/closure-archive=failed"})
							if err != nil {
								t.Fatal(err)
							}
							if len(archives.Items) != 1 {
								t.Errorf("restart %d: want one durable failure archive, got %d", restart, len(archives.Items))
							} else {
								record := &contracts.FailedIncidentClosure{}
								if err := wire.Unmarshal([]byte(archives.Items[0].Data[lifecycleio.FailedClosureDataKey]), record); err != nil {
									t.Fatal(err)
								}
								if record.GetClosure().GetRequest().GetIncidentId() != request.IncidentID || !record.GetFailure().GetPermanent() {
									t.Errorf("stale evidence not archived: %v", record)
								}
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
		})
	}
}

// A lost responder is replayable, but maintenance must retain it until resume.
// This covers the same startup seam without a source/policy advance.
func TestRV6PausedStartupWorkerLossWaitsForResume(t *testing.T) {
	now := time.Now().UTC()
	cfg := testControllerConfig()
	for _, phase := range []contracts.DispatchState{
		contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING,
		contracts.DispatchState_DISPATCH_STATE_RUNNING,
	} {
		t.Run(phase.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			initial, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := initial.incidentRequest(now, []sdk.Finding{stateFinding("health")})
			request.DetectorHistory = testDetectorHistory(now)
			initial.currentIncidentRequest, initial.incidentOpen = &request, true
			initial.dispatchState = phase
			initial.incidentDetectedAt, initial.incidentDispatchedAt = now, now
			client := fake.NewSimpleClientset()
			store := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "state")
			if _, err := store.Save(ctx, exportState(t, initial), ""); err != nil {
				t.Fatal(err)
			}
			for restart := 1; restart <= 3; restart++ {
				detector := controllerDetector("health", time.Second, sdk.Finding{})
				detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
				detector.spec.OriginatingCommit = "test-health"
				detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
				detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
				dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
				restored, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{}, dispatcher, now)
				if err != nil {
					t.Fatal(err)
				}
				if err := restored.AttachStateStore(ctx, store); err != nil {
					t.Fatal(err)
				}
				if err := restored.SetIncidentBroker(&recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/tmp/replayed-workspace", BaseCommit: cfg.SourceCommit}}); err != nil {
					t.Fatal(err)
				}
				if err := restored.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				if err := executeStartupEffects(ctx, restored, true); err != nil {
					t.Fatal(err)
				}
				if err := executePausedEffects(ctx, restored); err != nil {
					t.Fatal(err)
				}
				if detector.calls != 0 || len(dispatcher.requests) != 0 || len(restored.workspaceResults) != 0 || len(restored.results) != 0 {
					t.Fatal("paused startup replayed lost worker or evaluated application")
				}
				saved, _, err := store.Load(ctx)
				if err != nil || !saved.GetIncidentOpen() || saved.GetIncidentRequest().GetIncidentId() != request.IncidentID {
					t.Fatalf("lost interrupted investigation during pause: %v %v", saved, err)
				}
				if restart == 3 {
					// Resume uses a fresh application evaluation, then replays the lost
					// workspace/worker using its original identity and response deadline.
					if err := restored.Step(ctx, now.Add(time.Second), nil); err != nil {
						t.Fatal(err)
					}
					if err := restored.PersistState(ctx); err != nil {
						t.Fatal(err)
					}
					if err := executeStartupEffects(ctx, restored, false); err != nil {
						t.Fatal(err)
					}
					if phase == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING {
						select {
						case completion := <-restored.workspaceResults:
							restored.handleWorkspaceCompletion(completion)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						if err := restored.PersistState(ctx); err != nil {
							t.Fatal(err)
						}
						if err := executeStartupEffects(ctx, restored, false); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case replay := <-dispatcher.requests:
						if replay.IncidentID != request.IncidentID || !replay.ResponseDeadline.Equal(request.ResponseDeadline) {
							t.Fatal("worker replay changed identity or budget")
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case completion := <-restored.results:
						restored.handleDispatchCompletion(completion, now.Add(time.Second))
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					for _, at := range []time.Duration{time.Minute, time.Minute + restored.config.ClosureStabilityWindow} {
						if err := restored.Step(ctx, now.Add(at), nil); err != nil {
							t.Fatal(err)
						}
					}
					if detector.calls == 0 || restored.incidentOpen || restored.pendingClosure == nil || len(restored.pendingClosure.FinalDetectorStates) != 1 || restored.pendingClosure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
						t.Fatal("resumed investigation did not reach fresh verification")
					}
					t.Logf("restart=%d worker=%s terminal=fresh_verification", restart, phase)
				}
			}
		})
	}
}

func TestRV6PausedStartupRetirementPersistenceFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	root, _, _, _, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingSwap(t, root)
	current := tracingRefresh(t, root, coordinator)
	now := time.Now().UTC()
	cfg := testControllerConfig()
	cfg.SourceCommit = current.Commit
	c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
	request.SourceCommit = old.Commit
	request.DetectorHistory = testDetectorHistory(now)
	c.currentIncidentRequest, c.incidentOpen = &request, true
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "state")
	if _, err := store.Save(ctx, exportState(t, c), ""); err != nil {
		t.Fatal(err)
	}
	if err := c.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	rv6Guard(c, root, current.Commit)
	failure := errors.New("retirement persistence unavailable")
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) { return true, nil, failure })
	if err := executeStartupEffects(ctx, c, true); !errors.Is(err, failure) {
		t.Fatalf("want visible persistence failure, got %v", err)
	}
	saved, _, err := store.Load(ctx)
	if err != nil || !saved.GetIncidentOpen() || len(saved.GetFailedClosures()) != 0 {
		t.Fatalf("failed save claimed durable retirement: %v %v", saved, err)
	}
}
