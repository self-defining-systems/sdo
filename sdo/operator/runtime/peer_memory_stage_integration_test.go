//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Both controllers use the real native broker and the same accepted repository.
// Only the peer's compiler is gated: workspace preparation and the deterministic
// stage must reach real plan writes before that independent publication finishes.
func TestPeerMemoryPublicationNeverBlocksBoundStage(t *testing.T) {
	for _, schedule := range []string{"validating", "committed"} {
		t.Run(schedule, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			validating, release := make(chan struct{}), make(chan struct{})
			reflectionReady, beginReflection := make(chan struct{}), make(chan struct{})
			var once, beginOnce sync.Once
			begin := func() { beginOnce.Do(func() { close(beginReflection) }) }
			unblock := func() { begin(); once.Do(func() { close(release) }) }
			defer unblock()
			peerRepository, err := broker.New(root, t.TempDir(), func(ctx context.Context, _ string, paths []string) error {
				memory := false
				for _, path := range paths {
					memory = memory || strings.HasPrefix(path, ".sdo/")
				}
				if !memory {
					return nil
				}
				close(validating)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			peerBroker := NativeIncidentBroker{Repository: peerRepository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none", Reflector: ReflectionWorkerFunc(func(ctx context.Context, _ IncidentClosure, workspace IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
				close(reflectionReady)
				select {
				case <-beginReflection:
				case <-ctx.Done():
					return ReflectionDecision{}, ctx.Err()
				}
				path := filepath.Join(workspace.Worktree, ".sdo/playbooks/base/README.md")
				body, err := os.ReadFile(path)
				if err != nil {
					return ReflectionDecision{}, err
				}
				if err := os.WriteFile(path, append(body, []byte("Synthetic peer observation for <RESOURCE_NAME>.\n")...), 0644); err != nil {
					return ReflectionDecision{}, err
				}
				return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, nil
			})}
			health := newChaosHealthDetector("health")
			health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
			health.active.set(true)
			start := time.Now().UTC()
			cfg := testControllerConfig()
			cfg.RepositoryWorktree, cfg.SourceCommit, cfg.DeployedCommit = root, base, base
			cfg.BatchDebounce, cfg.FiringThreshold, cfg.ClearThreshold = 0, 1, 1
			peer, err := NewController(cfg, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, sharedMemoryResponder{health: health, lane: "peer", start: start}, start)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { unblock(); cancel(); peer.Wait() }()
			if err := peer.SetIncidentBroker(peerBroker); err != nil {
				t.Fatal(err)
			}
			clock := newSimClock()
			clock.Set(start)
			peer.now = clock.Now
			for tick := 0; tick < 30; tick++ {
				clock.Set(start.Add(time.Duration(tick) * time.Second))
				if err := peer.Step(ctx, clock.Now(), nil); err != nil {
					t.Fatal(err)
				}
				if effect, ok := peer.PendingWorkspaceEffect(); ok {
					if err := peer.ExecuteWorkspaceEffect(ctx, effect); err != nil {
						t.Fatal(err)
					}
					awaitBrokerResult(t, peer.workspaceResults)
					peer.processBrokerCompletions()
				}
				if _, ok := peer.PendingDispatchEffect(); ok {
					executePendingEffect(t, peer)
					awaitDispatchCompletionQueued(t, peer)
				}
				if _, ok := peer.PendingClosureEffect(); ok {
					break
				}
			}
			closure, ok := peer.PendingClosureEffect()
			if !ok {
				t.Fatal("peer never reached verified closure")
			}
			if err := peer.ExecuteClosureEffect(ctx, closure); err != nil {
				t.Fatal(err)
			}
			select {
			case <-reflectionReady:
			case result := <-peer.closureResults:
				t.Fatalf("peer did not reach reflection: %v", result.err)
			case <-ctx.Done():
				t.Fatal("peer did not reach reflection")
			}
			// Source repair and outcome are already accepted. Only the peer's
			// memory publication can move HEAD during the new incident below.
			base = nativeGit(t, root, "rev-parse", "HEAD")
			cfg.SourceCommit, cfg.DeployedCommit = base, base

			loop, state := pipelineTestLoop(t, false)
			dispatcher := &pipelineScenarioDispatcher{loop: loop}
			cfg.Application, cfg.Namespace, cfg.RepairPolicy = state.Request.Application, "app", "recorded-actions"
			controller, err := NewController(cfg, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { unblock(); cancel(); controller.Wait() }()
			repository, err := broker.New(root, t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.SetIncidentBroker(NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger")}); err != nil {
				t.Fatal(err)
			}
			if err := controller.Step(ctx, start, nil); err != nil {
				t.Fatal(err)
			}
			workspaceEffect, ok := controller.PendingWorkspaceEffect()
			if !ok {
				t.Fatal("incident not open before peer publication")
			}
			begin()
			select {
			case <-validating:
			case result := <-peer.closureResults:
				t.Fatalf("peer closed before publication validation: %v, receipt=%+v", result.err, result.receipt)
			case <-ctx.Done():
				t.Fatal("peer did not enter publication validation")
			}
			if schedule == "committed" {
				unblock()
				awaitBrokerResult(t, peer.closureResults)
				peer.processBrokerCompletions()
				if peer.closureReceipt == nil || peer.closureReceipt.ReflectionCommit == "" {
					t.Fatalf("peer publication failed: %+v", peer.closureFailure)
				}
				// A replacement process may have compiled newer accepted memory,
				// but workspace recovery must use the durable opening request.
				saved := exportState(t, controller)
				cfg.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
				restored, err := NewController(cfg, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
				if err != nil {
					t.Fatal(err)
				}
				if err := restored.RestoreState(saved); err != nil {
					t.Fatal(err)
				}
				if err := restored.SetIncidentBroker(controller.broker); err != nil {
					t.Fatal(err)
				}
				controller.Wait()
				controller = restored
			}
			if err := controller.ExecuteWorkspaceEffect(ctx, workspaceEffect); err != nil {
				t.Fatal(err)
			}
			select {
			case completion := <-controller.workspaceResults:
				if completion.err != nil {
					t.Fatal(completion.err)
				}
				if completion.workspace.BaseCommit != base || nativeGit(t, completion.workspace.Worktree, "rev-parse", "HEAD") != base {
					t.Fatalf("incident rebound from %s to peer head %s", base, completion.workspace.BaseCommit)
				}
				controller.handleWorkspaceCompletion(completion)
			case <-time.After(5 * time.Second):
				unblock()
				awaitBrokerResult(t, controller.workspaceResults)
				t.Fatal("incident workspace waited for peer publication validation")
			}
			executePendingEffect(t, controller)
			awaitDispatchCompletionQueued(t, controller)
			if got := len(loop.PlanApplications()); got != 2 {
				t.Fatalf("bound programs did not reach real stage writes: %d", got)
			}
			if schedule == "validating" {
				select {
				case <-release:
					t.Fatal("peer was released before the stage's writes")
				default:
				}
			}
			unblock()
			if schedule == "validating" {
				awaitBrokerResult(t, peer.closureResults)
				peer.processBrokerCompletions()
			}
			if peer.closureReceipt == nil || peer.closureReceipt.ReflectionCommit == "" {
				t.Fatalf("peer reflection did not publish: %+v", peer.closureFailure)
			}
			if head := nativeGit(t, root, "rev-parse", "HEAD"); head == base {
				t.Fatalf("peer did not advance accepted memory from %s", base)
			}
		})
	}
}

