package runtime

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

type planDryRunner interface {
	DryRunPatch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error
}

func (c KubernetesPlanClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	resource, err := c.resource(ref)
	if err != nil {
		return err
	}
	_, err = resource.Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
	return err
}
func (c TypedPlanClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.Client == nil {
		return fmt.Errorf("Kubernetes plan client is required")
	}
	options := metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}}
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		_, err := c.Client.CoreV1().Services(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
		return err
	case "v1/ConfigMap":
		_, err := c.Client.CoreV1().ConfigMaps(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
		return err
	case "apps/v1/Deployment":
		_, err := c.Client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, kind, patch, options)
		return err
	default:
		return fmt.Errorf("unsupported plan object %s/%s", ref.APIVersion, ref.Kind)
	}
}
