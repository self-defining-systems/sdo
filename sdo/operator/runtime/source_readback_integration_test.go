//go:build integration

package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"testing"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk/program"
)

// Real executor -> deterministic stage -> pending observer. Missing evidence
// never becomes a failed repair or success credit, and recovery never rewrites.
func TestSourceReadbackUnavailableRemainsPendingUntilObserved(t *testing.T) {
	for _, mode := range []string{"read-error", "identity-change", "source-error", "rollout-read-error"} {
		t.Run(mode, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, true)
			p := sourceConvergencePortProgram{}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
			if err != nil {
				t.Fatal(err)
			}
			loop.Programs = registry
			client := &sourceReadbackClient{PlanObjectClient: loop.Executor.Client, unavailable: true, mode: mode}
			loop.Executor.Client = client
			loop.Executor.Source = sourceReadbackSource{PlanSource: loop.Executor.Source, client: client}
			loop.Executor.Rollouts = sourceReadbackRollouts{PlanRollouts: loop.Executor.Rollouts, client: client}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			if len(state.PriorStages) != 1 || len(state.PriorStages[0].Mitigations) != 1 {
				t.Fatalf("missing stage result: %+v", state.PriorStages)
			}
			attempt := state.PriorStages[0].Mitigations[0]
			if attempt.Result != "rollout_pending_at_handoff" || attempt.ApplyReceipt.GetVerificationPassed() || attempt.ApplyReceipt.GetRolloutPending() == nil {
				t.Fatalf("missing readback became terminal: %v", attempt)
			}
			if err := wire.Validate(attempt.ApplyReceipt); err != nil {
				t.Fatal(err)
			}
			if len(loop.applicationsLog) != 1 || len(loop.pendingWrites) != 1 {
				t.Fatalf("pending attribution lost: %+v", loop.applicationsLog)
			}
			entry := loop.applicationsLog[0]
			if repairs := ProgramRepairReceipts(loop.applicationsLog, nil); len(repairs) == 0 || repairs[0].Success {
				t.Fatalf("unobserved repair credited: %+v", repairs)
			}
			// Clear the independent peer through the API. Healthy peers cannot
			// substitute for the missing evidence of this source write.
			typed := client.PlanObjectClient.(TypedPlanClient)
			peer, err := typed.Client.AppsV1().Deployments("app").Get(context.Background(), "api", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			peer.Spec.Template.Spec.Containers[0].Image = "good:v1"
			if _, err := typed.Client.AppsV1().Deployments("app").Update(context.Background(), peer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			loop.settlePending(context.Background())
			if loop.applications[entry.ID].VerificationPassed {
				t.Fatal("health bypassed missing readback")
			}
			client.unavailable = false
			loop.settlePending(context.Background())
			settled := loop.applications[entry.ID]
			if !settled.VerificationCompleted || !settled.VerificationPassed || settled.RolloutPending != nil || !settled.ActionReports[0].SourceReconcile.Converged {
				t.Fatalf("readback recovery failed to settle: %+v", settled)
			}
			// The actual final receipt producer feeds the reflection consumer.
			// Its earlier pending snapshot must not force a working step to change.
			settledResult, settledStages := loop.SettleRecoveryApplications(context.Background(), IncidentResult{PlanApplications: loop.ApplicationEvidence()}, state.PriorStages)
			result, err := incidentResultContract(settledResult)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, ".sdo/playbooks", p.Spec().ID), 0755); err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{PlanApplications: result.PlanApplications, PriorStages: settledStages}
			ds := validate.ReflectionRepairDiagnostics(root, outcome, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "existing step verified after readback recovered"}, nil)
			if len(ds) > 0 {
				t.Fatalf("settled repair forced memory churn: %v", ds)
			}
			if client.writes != 1 {
				t.Fatalf("observation repeated write: %d", client.writes)
			}
		})
	}
}
