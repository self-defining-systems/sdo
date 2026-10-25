package runtime

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// TypedPlanClient limits plan mutations to the application kinds the SDK reads.
type TypedPlanClient struct {
	Client     kubernetes.Interface
	ExecConfig *rest.Config
}

func (c TypedPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	var object any
	var err error
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Pod":
		object, err = c.Client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "apps/v1/StatefulSet":
		object, err = c.Client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/Service":
		object, err = c.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/ConfigMap":
		object, err = c.Client.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "apps/v1/Deployment":
		object, err = c.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration":
		if ref.Namespace != "" {
			return nil, fmt.Errorf("webhook configuration is cluster scoped")
		}
		object, err = c.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, ref.Name, metav1.GetOptions{})
	case "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration":
		if ref.Namespace != "" {
			return nil, fmt.Errorf("webhook configuration is cluster scoped")
		}
		object, err = c.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, ref.Name, metav1.GetOptions{})
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
	case "apps/v1/StatefulSet":
		_, err := c.Client.AppsV1().StatefulSets(ref.Namespace).Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
		return err
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

func (c TypedPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if c.Client == nil || object == nil || !samePlanRef(object, ref) || ref.Namespace == "" {
		return nil, fmt.Errorf("matching named object is required")
	}
	var result any
	var err error
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Pod":
		typed := new(corev1.Pod)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.CoreV1().Pods(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "v1/ConfigMap":
		typed := new(corev1.ConfigMap)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.CoreV1().ConfigMaps(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "v1/Service":
		typed := new(corev1.Service)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.CoreV1().Services(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "apps/v1/Deployment":
		typed := new(appsv1.Deployment)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.AppsV1().Deployments(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "apps/v1/StatefulSet":
		typed := new(appsv1.StatefulSet)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.AppsV1().StatefulSets(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "networking.k8s.io/v1/NetworkPolicy":
		typed := new(networkingv1.NetworkPolicy)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.NetworkingV1().NetworkPolicies(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "autoscaling/v2/HorizontalPodAutoscaler":
		typed := new(autoscalingv2.HorizontalPodAutoscaler)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.AutoscalingV2().HorizontalPodAutoscalers(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	case "policy/v1/PodDisruptionBudget":
		typed := new(policyv1.PodDisruptionBudget)
		if err = kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err == nil {
			result, err = c.Client.PolicyV1().PodDisruptionBudgets(ref.Namespace).Create(ctx, typed, metav1.CreateOptions{})
		}
	default:
		return nil, fmt.Errorf("object create transport is unavailable for %s/%s", ref.APIVersion, ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	return planUnstructured(result, ref)
}

func (c TypedPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if c.Client == nil || uid == "" || version == "" {
		return fmt.Errorf("delete requires current UID and resource version")
	}
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Pod":
		return c.Client.CoreV1().Pods(ref.Namespace).Delete(ctx, ref.Name, options)
	case "v1/ConfigMap":
		return c.Client.CoreV1().ConfigMaps(ref.Namespace).Delete(ctx, ref.Name, options)
	case "v1/Service":
		return c.Client.CoreV1().Services(ref.Namespace).Delete(ctx, ref.Name, options)
	case "apps/v1/Deployment":
		return c.Client.AppsV1().Deployments(ref.Namespace).Delete(ctx, ref.Name, options)
	case "apps/v1/StatefulSet":
		return c.Client.AppsV1().StatefulSets(ref.Namespace).Delete(ctx, ref.Name, options)
	case "networking.k8s.io/v1/NetworkPolicy":
		return c.Client.NetworkingV1().NetworkPolicies(ref.Namespace).Delete(ctx, ref.Name, options)
	case "autoscaling/v2/HorizontalPodAutoscaler":
		return c.Client.AutoscalingV2().HorizontalPodAutoscalers(ref.Namespace).Delete(ctx, ref.Name, options)
	case "policy/v1/PodDisruptionBudget":
		return c.Client.PolicyV1().PodDisruptionBudgets(ref.Namespace).Delete(ctx, ref.Name, options)
	}
	return fmt.Errorf("object delete transport is unavailable for %s/%s", ref.APIVersion, ref.Kind)
}

// Exec requests no stdout/stderr streams. Kubernetes requires at least one I/O
// stream, so stdin is opened and immediately closed at EOF. Existing RBAC applies.
func (c TypedPlanClient) Exec(ctx context.Context, pod sdk.ObjectRef, container string, command []string) (int, error) {
	code, err := c.runCommand(ctx, program.Command{Pod: pod, Container: container, Args: command})
	return int(code), err
}
