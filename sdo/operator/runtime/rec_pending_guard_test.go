package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

type recTimeoutRollout struct{}

func (recTimeoutRollout) Wait(context.Context, []sdk.ObjectRef) error {
	return context.DeadlineExceeded
}

type recAmbiguousMenuClient struct {
	*menuB3Cluster
	uncertain bool
}

func (c *recAmbiguousMenuClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.menuB3Cluster.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	c.uncertain = true
	return context.DeadlineExceeded // The server applied it; the response was lost.
}

func (c *recAmbiguousMenuClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.uncertain {
		return nil, context.DeadlineExceeded
	}
	return c.menuB3Cluster.Get(ctx, ref)
}

