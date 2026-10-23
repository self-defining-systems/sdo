package runtime

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
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
func (c healthGuardedPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return err
		}
	}
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return fmt.Errorf("plan client does not support the requested capability")
	}
	return base.Delete(ctx, ref, uid, version)
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

func (c healthGuardedPlanClient) Exec(ctx context.Context, ref sdk.ObjectRef, container string, command []string) (int, error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return 0, err
		}
	}
	base, ok := c.PlanObjectClient.(PlanExecClient)
	if !ok {
		return 0, fmt.Errorf("plan client does not support exec")
	}
	return base.Exec(ctx, ref, container, command)
}
