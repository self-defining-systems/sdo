package runtime

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// Forward ordinary executor capabilities through its health guard.
func (c healthGuardedPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return nil, err
		}
	}
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return nil, fmt.Errorf("plan client does not support the requested capability")
	}
	return base.Create(ctx, ref, object)
}
func (c healthGuardedPlanClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return "", err
		}
	}
	base, ok := c.PlanObjectClient.(PlanIdentityInspector)
	if !ok {
		return "", fmt.Errorf("plan client does not support the requested capability")
	}
	return base.TokenlessServiceAccount(ctx, ref)
}
