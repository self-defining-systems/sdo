package runtime

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

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
