package runtime

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// Forward lifecycle operations into the existing executor client. The menu
// authorizes and journals effects; PlanExecutor owns their semantics/rollback.
func (c *menuPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return nil, errMenuSafety
	}
	l := c.loop
	if l.menuFence(ctx) != nil {
		return nil, errSelectionFenced
	}
	if _, err := l.menuSnapshot(ctx); err != nil {
		return nil, errMenuSafety
	}
	if c.validatePostImage(ctx, nil, object) != nil || l.MenuConfig.Safety.object(ctx, ref, object) != nil || l.MenuConfig.Safety.AuthorizeEffect(ctx, ref, nil, object.DeepCopy()) != nil {
		return nil, errMenuSafety
	}
	if _, err := c.Get(ctx, ref); !apierrors.IsNotFound(err) {
		return nil, errMenuSafety
	}
	id, err := c.beginLifecycle(ctx, ref, "create")
	if err != nil {
		return nil, err
	}
	if l.menuFence(ctx) != nil {
		return nil, errSelectionFenced
	}
	created, err := base.Create(ctx, ref, object.DeepCopy())
	if err != nil {
		return nil, errMenuSafety
	}
	if created == nil || created.GetUID() == "" || created.GetResourceVersion() == "" || l.MenuConfig.Safety.object(ctx, ref, created) != nil {
		return nil, errMenuSafety
	}
	if err := l.selection.completeAction(ctx, c.attempt, id, c.phase == "compensation", string(created.GetUID()), created.GetResourceVersion()); err != nil {
		return nil, err
	}
	return created, nil
}
func (c *menuPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return errMenuSafety
	}
	l := c.loop
	if l.menuFence(ctx) != nil {
		return errSelectionFenced
	}
	if _, err := l.menuSnapshot(ctx); err != nil {
		return errMenuSafety
	}
	before, err := c.Get(ctx, ref)
	if err != nil {
		return err
	}
	if before.GetUID() != uid || before.GetResourceVersion() != version || l.MenuConfig.Safety.AuthorizeEffect(ctx, ref, before.DeepCopy(), nil) != nil {
		return errMenuSafety
	}
	id, err := c.beginLifecycle(ctx, ref, "delete")
	if err != nil {
		return err
	}
	if l.menuFence(ctx) != nil {
		return errSelectionFenced
	}
	if err := base.Delete(ctx, ref, uid, version); err != nil {
		return errMenuSafety
	}
	// API acceptance is an applied request, not proof of completed deletion.
	// The executor and causal barrier must independently confirm named absence.
	return l.selection.completeAction(ctx, c.attempt, id, c.phase == "compensation", string(uid), version)
}
func (c *menuPlanClient) beginLifecycle(ctx context.Context, ref sdk.ObjectRef, operation string) (string, error) {
	id := ""
	if c.phase == "compensation" {
		if len(c.compensation) == 0 {
			return "", errMenuSafety
		}
		id = c.compensation[0]
		c.compensation = c.compensation[1:]
	} else {
		c.next++
		id = menuActionID(c.attempt, c.round, c.next)
	}
	receipt := menuActionReceipt(id, ref)
	receipt.ActionKind = operation
	receipt.ChangedFields = []string{"object"}
	return id, c.loop.selection.beginAction(ctx, c.attempt, c.round, c.phase, receipt)
}

// Capability forwarding keeps lifecycle and identity operations behind the
// existing health/epoch guards without changing their owning implementation.
func (c healthGuardedPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if c.Check != nil {
		if err := c.Check(ctx); err != nil {
			return nil, err
		}
	}
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return nil, errMenuSafety
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
		return errMenuSafety
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
		return "", errMenuSafety
	}
	return base.TokenlessServiceAccount(ctx, ref)
}
func (c selectionFencedPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if err := c.journal.authorizeMutation(ctx, c.epoch); err != nil {
		return nil, err
	}
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return nil, errMenuSafety
	}
	return base.Create(ctx, ref, object)
}
func (c selectionFencedPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if err := c.journal.authorizeMutation(ctx, c.epoch); err != nil {
		return err
	}
	base, ok := c.PlanObjectClient.(PlanLifecycleClient)
	if !ok {
		return errMenuSafety
	}
	return base.Delete(ctx, ref, uid, version)
}
func (c selectionFencedPlanClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	if err := c.journal.currentEpoch(ctx, c.epoch); err != nil {
		return "", err
	}
	base, ok := c.PlanObjectClient.(PlanIdentityInspector)
	if !ok {
		return "", errMenuSafety
	}
	return base.TokenlessServiceAccount(ctx, ref)
}
