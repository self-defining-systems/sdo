package runtime

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// Delete preserves the health gate and the underlying identity preconditions.
func (c healthGuardedPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return err
		}
	}
	client, ok := c.PlanObjectClient.(planPodDeleter)
	if !ok {
		return fmt.Errorf("plan client does not support conditional deletion")
	}
	return client.Delete(ctx, ref, uid, version)
}

// Exec preserves the health gate without changing transport identity or RBAC.
func (c healthGuardedPlanClient) Exec(ctx context.Context, ref sdk.ObjectRef, container string, command []string) (int, error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return 0, err
		}
	}
	client, ok := c.PlanObjectClient.(PlanExecClient)
	if !ok {
		return 0, fmt.Errorf("plan client does not support output-free exec")
	}
	return client.Exec(ctx, ref, container, command)
}
