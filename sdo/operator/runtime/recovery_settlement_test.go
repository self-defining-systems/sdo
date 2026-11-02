//go:build integration

package runtime

import (
	"context"
	"errors"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"reflect"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The actual stage producer must feed fresh closure evidence into novelty
// admission, rather than its earlier bounded rollout observation.
func TestRecoverySettlementFeedsNoveltyAdmission(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "converged", true: "failed-repair"}[failed], func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			rollout := &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
			if failed {
				rollout.failure = errors.New("synthetic execution rollout failed")
				client := loop.Executor.Client.(TypedPlanClient).Client.(*fake.Clientset)
				client.PrependReactor("patch", "*", func(ktesting.Action) (bool, kruntime.Object, error) {
					return true, nil, errors.New("synthetic execution write failed")
				})
			}
			loop.Executor.Rollouts = rollout
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{ObservationCeiling: time.Millisecond, NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				before.ResponderSessionID = "11111111-2222-4333-8444-555555555555"
				before.ResolutionPath = "responder"
			}
			if len(before.PlanApplications) == 0 || before.PlanApplications[0].Result.VerificationPassed {
				t.Fatal("fixture did not produce unsettled applications")
			}
			loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
			result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
			closure.Request.PriorStages = stages
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			if got := coveredRepeatCandidate(derived.Record); got == failed {
				t.Fatalf("novelty admission=%v failed=%v applications=%v", got, failed, derived.Record.PlanApplications)
			}
			if calls := settlementBrokerWorkers(t, loop, closure); (calls > 0) != failed {
				t.Fatalf("reflection workers=%d failed=%v", calls, failed)
			}
			for _, stage := range stages {
				for _, attempt := range stage.Mitigations {
					if !failed && attempt.Result != "applied_cleared" {
						t.Fatalf("stale attempt: %v", attempt)
					}
					if failed && attempt.ApplyReceipt.GetVerification().GetResult() == contracts.Verdict_RESULT_NO_MATCH {
						t.Fatal("failed rollout credited")
					}
				}
			}
		})
	}
}

// Keep the broker's full no-new-evidence Check in the path: admission alone
// cannot prove reflection was suppressed or that real failures still learn.
func settlementBrokerWorkers(t *testing.T, loop *ResponderLoop, closure IncidentClosure) int {
	t.Helper()
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, match := range derived.Record.GetPlaybookCoverage().GetMatches() {
		id := match.PlaybookId
		t1Write(t, root, ".sdo/playbooks/"+id+"/README.md", []byte("---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: initial\n---\nSynthetic accepted repair <RESOURCE_NAME>\n"))
	}
	if _, err := layout.WritePlaybookIndex(root); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "synthetic accepted repairs")
	repository, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Replay: "off", Deriver: ProtoOutcomeDeriver{}, Programs: loop.Programs, ResponderBackend: "test", ResponderModel: "test", Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		return ReflectionDecision{LearningDecision: "failed", Error: "synthetic reflection observed unsettled work"}, nil
	})}
	workspace, err := native.PrepareIncident(context.Background(), closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure.Request.RepositoryBaseCommit = workspace.BaseCommit
	closure.Request.RepositoryWorktree = workspace.Worktree
	receipt, err := native.ProcessClosure(context.Background(), closure)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.AcknowledgeClosure(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if err := native.ProcessQueuedReflection(context.Background(), closure.Request.IncidentID); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		ledger, err := native.readLedger(closure.Request.IncidentID)
		if err != nil {
			t.Fatal(err)
		}
		if ledger.GetReflectionDecision().GetLearningDecision() != "no_change" || ledger.GetReflectionDecision().GetNoChangeReason() != "accepted_programs_no_new_evidence" {
			t.Fatalf("zero workers without broker admission: %v", ledger.GetReflectionDecision())
		}
	}
	return calls
}

func TestControllerRecoveryProofSettlesStageReceipts(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
	dispatcher := &settlementControllerDispatcher{pipelineScenarioDispatcher: pipelineScenarioDispatcher{loop: loop}}
	config := testControllerConfig()
	config.Application = state.Request.Application
	config.Namespace = "app"
	config.SourceCommit = state.Request.SourceCommit
	config.DeployedCommit = state.Request.DeployedCommit
	config.RepositoryWorktree = t.TempDir()
	config.RepairPolicy = "recorded-actions"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Millisecond
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	controller.SetLifecycleSink(&memoryLifecycleSink{})
	// Production LoopFactory registers this loop when the stage is built.
	dispatcher.controller = controller
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	for i := 0; i < 3; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("stage not admitted")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitDispatchCompletionQueued(t, controller)
	applications := loop.PlanApplications()
	if len(applications) != 2 || applications[0].Result.RolloutPending == nil {
		t.Fatal("stage did not hand off pending evidence")
	}
	observer := observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
	if err := observer.Wait(context.Background(), []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}); err != nil {
		t.Fatal(err)
	}
	loop.Executor.Rollouts = observer
	for i := 3; i < 23; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case closure := <-closed:
		if dispatcher.agentCalls != 0 {
			t.Fatal("covered repairs dispatched a responder")
		}
		outcome, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
		if err != nil {
			t.Fatal(err)
		}
		if !coveredRepeatCandidate(outcome.Record) {
			t.Fatalf("controller froze unsettled receipts: %v", outcome.Record)
		}
	default:
		t.Fatal("controller recovery did not close")
	}
}

type settlementControllerDispatcher struct {
	pipelineScenarioDispatcher
	controller *Controller
}

func (d *settlementControllerDispatcher) DeterministicResponseStage() ResponseStage {
	return &DeterministicResponseStage{ObservationCeiling: time.Millisecond, NewLoop: func(request IncidentRequest) *ResponderLoop {
		d.loop.Request = request
		d.controller.mu.Lock()
		d.controller.responderLoop = d.loop
		d.controller.mu.Unlock()
		return d.loop
	}}
}

func TestRecoverySettlementPreservesIndependentReceiptIdentity(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{ObservationCeiling: time.Millisecond, NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	result, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	independent := RepairActionReceipt{ActionID: "sdo-program-" + result.PlanApplications[0].ID + "-independent", Kind: "recorded-action", Summary: "independent write must remain evidence", Success: true, Resources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "peer"}}}
	result.RepairActions = append(result.RepairActions, independent)
	settled, _ := loop.SettleRecoveryApplications(context.Background(), result, state.PriorStages)
	for _, receipt := range settled.RepairActions {
		if receipt.ActionID == independent.ActionID && reflect.DeepEqual(receipt, independent) {
			return
		}
	}
	t.Fatal("settlement deleted an independent repair merely sharing a controller receipt prefix")
}
