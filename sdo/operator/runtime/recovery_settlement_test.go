package runtime

import (
	"context"
	"errors"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory/layout"
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
			}
			loop.Executor.Rollouts = rollout
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
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
	for _, match := range closure.Request.PlaybookCoverage.Matches {
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
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	return calls
}
