package runtime

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/sdk"
)

// TypedPlanClient limits plan mutations to the application kinds the SDK reads.
type TypedPlanClient struct{ Client kubernetes.Interface }

func (c TypedPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	var object any
	var err error
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		object, err = c.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/ConfigMap":
		object, err = c.Client.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "apps/v1/Deployment":
		object, err = c.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	default:
		return nil, fmt.Errorf("plan object kind %s/%s is unsupported", ref.APIVersion, ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	return planUnstructured(object, ref)
}

func (c TypedPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.Client == nil {
		return fmt.Errorf("Kubernetes plan client is required")
	}
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		_, err := c.Client.CoreV1().Services(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	case "v1/ConfigMap":
		_, err := c.Client.CoreV1().ConfigMaps(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	case "apps/v1/Deployment":
		_, err := c.Client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	default:
		return fmt.Errorf("plan object kind %s/%s is unsupported", ref.APIVersion, ref.Kind)
	}
}
