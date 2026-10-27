package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type sourceConvergencePortProgram struct{ pipelinePortProgram }

func (sourceConvergencePortProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["service"]}}}, nil
}

type sourceConvergenceAPIClient struct {
	PlanObjectClient
	afterPatch func(context.Context, sdk.ObjectRef) error
}

func (c sourceConvergenceAPIClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.PlanObjectClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	return c.afterPatch(ctx, ref)
}

// Real executor -> real responder loop -> real deterministic-stage receipt.
// A successful transport write with admission reintroducing drift cannot clear
// the stage finding or hide the remaining paths from the following responder.
func TestSourceReconcileStageHandsTypedPartialToResponder(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	p := sourceConvergencePortProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	typed := loop.Executor.Client.(TypedPlanClient)
	loop.Executor.Client = sourceConvergenceAPIClient{PlanObjectClient: typed, afterPatch: func(ctx context.Context, ref sdk.ObjectRef) error {
		service, err := typed.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		_, err = typed.Client.CoreV1().Services(ref.Namespace).Update(ctx, service, metav1.UpdateOptions{})
		return err
	}}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
		t.Fatal(err)
	}
	if len(state.Findings) != len(state.Request.Findings) || len(state.PriorStages) != 1 || len(state.PriorStages[0].Mitigations) != 1 {
		t.Fatalf("partial source repair lost its finding/stage: findings=%d stages=%d", len(state.Findings), len(state.PriorStages))
	}
	attempt := state.PriorStages[0].Mitigations[0]
	if attempt.Result == "applied_cleared" || attempt.ApplyReceipt.GetVerificationPassed() || attempt.Error == "" {
		t.Fatalf("partial source repair received stage clearance: %v", attempt)
	}
	reports := attempt.ApplyReceipt.GetActionReports()
	if len(reports) != 1 || reports[0].GetStatus() != "partial" || len(reports[0].GetSourceReconcile().GetResidualPaths()) == 0 {
		t.Fatalf("responder stage receipt lost typed residual: %v", attempt)
	}
}

func TestSourceReconcileApplyResponseCarriesTypedResidual(t *testing.T) {
	loop, _ := pipelineTestLoop(t, true)
	p := sourceConvergencePortProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	typed := loop.Executor.Client.(TypedPlanClient)
	loop.Executor.Client = sourceConvergenceAPIClient{PlanObjectClient: typed, afterPatch: func(ctx context.Context, ref sdk.ObjectRef) error {
		service, err := typed.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		_, err = typed.Client.CoreV1().Services(ref.Namespace).Update(ctx, service, metav1.UpdateOptions{})
		return err
	}}
	loop.RefreshDiagnose(context.Background())
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: program.Bindings{Objects: map[string]sdk.ObjectRef{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	encoded, err := json.Marshal(applied)
	if err != nil || !strings.Contains(string(encoded), "source_reconcile") || !strings.Contains(string(encoded), "residual_paths") {
		t.Fatalf("apply response lost residual: %s %v", encoded, err)
	}
	if len(applied.ActionReports) != 1 || applied.ActionReports[0].Status != "partial" {
		t.Fatalf("apply response lost partial status: %+v", applied.ActionReports)
	}
}
