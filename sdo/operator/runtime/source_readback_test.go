package runtime

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type sourceReadbackClient struct {
	PlanObjectClient
	writes      int
	unavailable bool
	mode        string
}

func (c *sourceReadbackClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.writes > 0 && c.unavailable && (c.mode == "read-error" || c.mode == "identity-change") {
		if c.mode == "read-error" {
			return nil, errors.New("synthetic readback outage")
		}
		object, err := c.PlanObjectClient.Get(ctx, ref)
		if err == nil {
			object.SetUID("replacement")
		}
		return object, err
	}
	return c.PlanObjectClient.Get(ctx, ref)
}

func (c *sourceReadbackClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.PlanObjectClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	c.writes++
	return nil
}

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
			result, err := incidentResultContract(IncidentResult{PlanApplications: loop.PlanApplications()})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, ".sdo/playbooks", p.Spec().ID), 0755); err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{PlanApplications: result.PlanApplications, PriorStages: state.PriorStages}
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

// The other readback boundaries obey the same observation-only contract.
type sourceReadbackSource struct {
	PlanSource
	client *sourceReadbackClient
}

func (s sourceReadbackSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if s.client.writes > 0 && s.client.unavailable && s.client.mode == "source-error" {
		return nil, errors.New("synthetic source outage")
	}
	return s.PlanSource.AcceptedManifest(ctx, revision, ref)
}

type sourceReadbackRollouts struct {
	PlanRollouts
	client *sourceReadbackClient
}

func (r sourceReadbackRollouts) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	if r.client.unavailable && r.client.mode == "rollout-read-error" {
		return nil, errors.New("synthetic rollout read outage")
	}
	return nil, nil
}
