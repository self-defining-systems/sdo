package runtime

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

type stageIndexedRollout struct {
	base         PlanRollouts
	failureIndex int
	calls        int
}

func (r *stageIndexedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	index := r.calls
	r.calls++
	if index == r.failureIndex {
		return errors.New("synthetic rollout failure")
	}
	return r.base.Wait(ctx, refs)
}

type stageFailedWriteClient struct{ PlanObjectClient }

func (c stageFailedWriteClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if ref.Kind == "Deployment" {
		return errors.New("synthetic Deployment write refused")
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, data)
}
