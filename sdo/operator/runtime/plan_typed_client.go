package runtime

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
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
	case "networking.k8s.io/v1/NetworkPolicy":
		object, err = c.Client.NetworkingV1().NetworkPolicies(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "autoscaling/v2/HorizontalPodAutoscaler":
		object, err = c.Client.AutoscalingV2().HorizontalPodAutoscalers(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "policy/v1/PodDisruptionBudget":
		object, err = c.Client.PolicyV1().PodDisruptionBudgets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
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
	case "networking.k8s.io/v1/NetworkPolicy":
		_, err := c.Client.NetworkingV1().NetworkPolicies(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	case "autoscaling/v2/HorizontalPodAutoscaler":
		_, err := c.Client.AutoscalingV2().HorizontalPodAutoscalers(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	case "policy/v1/PodDisruptionBudget":
		_, err := c.Client.PolicyV1().PodDisruptionBudgets(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
	default:
		return fmt.Errorf("plan object kind %s/%s is unsupported", ref.APIVersion, ref.Kind)
	}
}

func planUnstructured(object any, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: fields}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	return result, nil
}

// TokenlessServiceAccount returns only identity metadata after rejecting token
// references and implicit automount. It never reads a referenced Secret.
func (c TypedPlanClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	if c.Client == nil || ref.APIVersion != "v1" || ref.Kind != "ServiceAccount" {
		return "", fmt.Errorf("existing service account reader is unavailable")
	}
	account, err := c.Client.CoreV1().ServiceAccounts(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("existing service account is unavailable")
	}
	if account.UID == "" || account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken || len(account.Secrets) > 0 || len(account.ImagePullSecrets) > 0 {
		return "", fmt.Errorf("service account is token-bearing or unproven")
	}
	return string(account.UID), nil
}
