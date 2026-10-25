package runtime

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

// GenericPlanClient uses discovery for object operations, retaining the existing
// typed client only for runtime operations such as exec and revision rollback.
type GenericPlanClient struct {
	TypedPlanClient
	Objects *core.GenericCapture
}

func (c GenericPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.Objects == nil {
		return c.TypedPlanClient.Get(ctx, ref)
	}
	resource, err := c.Objects.Resource(ref)
	if err != nil {
		return nil, err
	}
	return resource.Get(ctx, ref.Name, metav1.GetOptions{})
}
func (c GenericPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.Objects == nil {
		return c.TypedPlanClient.Patch(ctx, ref, kind, patch)
	}
	resource, err := c.Objects.Resource(ref)
	if err != nil {
		return err
	}
	_, err = resource.Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
	return err
}
func (c GenericPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if c.Objects == nil {
		return c.TypedPlanClient.Create(ctx, ref, object)
	}
	if object == nil || !samePlanRef(object, ref) {
		return nil, fmt.Errorf("create requires matching object identity")
	}
	resource, err := c.Objects.Resource(ref)
	if err != nil {
		return nil, err
	}
	return resource.Create(ctx, object, metav1.CreateOptions{})
}
func (c GenericPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if c.Objects == nil {
		return c.TypedPlanClient.Delete(ctx, ref, uid, version)
	}
	if uid == "" || version == "" {
		return fmt.Errorf("delete requires UID and resource version")
	}
	resource, err := c.Objects.Resource(ref)
	if err != nil {
		return err
	}
	return resource.Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
}
