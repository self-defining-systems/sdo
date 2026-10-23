package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/sdk"
)

// Use the controller's existing transport and credentials. This has exactly
// the dynamic reader's most-recent, bounded, complete-list semantics.
func (r KubernetesCurrentReader) listTypedCurrent(ctx context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	options := metav1.ListOptions{Limit: 1001}
	var list kruntime.Object
	var err error
	client := r.TypedClient
	switch kind.APIVersion + "/" + kind.Kind {
	case "v1/ConfigMap":
		list, err = client.CoreV1().ConfigMaps(kind.Namespace).List(ctx, options)
	case "v1/Service":
		list, err = client.CoreV1().Services(kind.Namespace).List(ctx, options)
	case "v1/Pod":
		list, err = client.CoreV1().Pods(kind.Namespace).List(ctx, options)
	case "v1/Endpoints":
		list, err = client.CoreV1().Endpoints(kind.Namespace).List(ctx, options)
	case "v1/Event":
		list, err = client.CoreV1().Events(kind.Namespace).List(ctx, options)
	case "apps/v1/Deployment":
		list, err = client.AppsV1().Deployments(kind.Namespace).List(ctx, options)
	case "apps/v1/ReplicaSet":
		list, err = client.AppsV1().ReplicaSets(kind.Namespace).List(ctx, options)
	case "apps/v1/StatefulSet":
		list, err = client.AppsV1().StatefulSets(kind.Namespace).List(ctx, options)
	case "discovery.k8s.io/v1/EndpointSlice":
		list, err = client.DiscoveryV1().EndpointSlices(kind.Namespace).List(ctx, options)
	case "networking.k8s.io/v1/NetworkPolicy":
		list, err = client.NetworkingV1().NetworkPolicies(kind.Namespace).List(ctx, options)
	case "autoscaling/v2/HorizontalPodAutoscaler":
		list, err = client.AutoscalingV2().HorizontalPodAutoscalers(kind.Namespace).List(ctx, options)
	case "policy/v1/PodDisruptionBudget":
		list, err = client.PolicyV1().PodDisruptionBudgets(kind.Namespace).List(ctx, options)
	default:
		return CurrentObjectList{}, errCausalObservation
	}
	if err != nil {
		return CurrentObjectList{}, errCausalObservation
	}
	metadata, err := meta.ListAccessor(list)
	if err != nil {
		return CurrentObjectList{}, errCausalObservation
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return CurrentObjectList{}, errCausalObservation
	}
	result := CurrentObjectList{ResourceVersion: metadata.GetResourceVersion(), Complete: metadata.GetContinue() == "" && (metadata.GetRemainingItemCount() == nil || *metadata.GetRemainingItemCount() == 0) && len(items) <= 1000}
	for _, item := range items {
		value, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(item)
		if err != nil {
			return CurrentObjectList{}, errCausalObservation
		}
		result.Items = append(result.Items, unstructured.Unstructured{Object: value})
	}
	return result, nil
}
