package runtime

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

type mapPlanSource map[sdk.ObjectRef]*unstructured.Unstructured

func (s mapPlanSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if object := s[ref]; object != nil {
		return object.DeepCopy(), nil
	}
	return nil, errors.New("missing source")
}

func (s mapPlanSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("no revision")
}

// hangingReadClient models an API read of one written object that does not
// return until its caller's deadline.
type hangingReadClient struct {
	*planFakeClient
	hang    sdk.ObjectRef
	patched map[sdk.ObjectRef]bool
}

func (c *hangingReadClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, bounded := ctx.Deadline(); bounded && ref == c.hang && c.patched[ref] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.planFakeClient.Get(ctx, ref)
}

func (c *hangingReadClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.patched[ref] = true
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
