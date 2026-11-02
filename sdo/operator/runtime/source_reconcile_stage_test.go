package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
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
