package runtime

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// SourceRolloutResiduals observes old-template capacity without changing it.
// Zero-sized historical ReplicaSets without Pods are normal rollout history.
func (w KubernetesPlanRollouts) SourceRolloutResiduals(ctx context.Context, observed *unstructured.Unstructured) ([]string, error) {
	if observed.GetAPIVersion() != "apps/v1" || observed.GetKind() != "Deployment" {
		return nil, nil
	}
	if w.Client == nil {
		return nil, fmt.Errorf("Kubernetes rollout client is required")
	}
	var deployment appsv1.Deployment
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(observed.Object, &deployment); err != nil {
		return nil, err
	}
	if deployment.UID == "" {
		return nil, fmt.Errorf("reconciled Deployment identity unavailable")
	}
	live, err := w.Client.AppsV1().Deployments(deployment.Namespace).Get(ctx, deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if live.UID != deployment.UID {
		return nil, fmt.Errorf("reconciled Deployment identity changed")
	}
	sets, err := w.Client.AppsV1().ReplicaSets(deployment.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	old := map[types.UID]string{}
	paths := map[string]bool{}
	for i := range sets.Items {
		rs := &sets.Items[i]
		owner := metav1.GetControllerOf(rs)
		if owner == nil || owner.UID != deployment.UID || owner.Name != deployment.Name || owner.Kind != "Deployment" || owner.APIVersion != "apps/v1" {
			continue
		}
		left, right := rs.Spec.Template.DeepCopy(), deployment.Spec.Template.DeepCopy()
		delete(left.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		delete(right.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
		if equality.Semantic.DeepEqual(left, right) {
			continue
		}
		if rs.UID == "" {
			return nil, fmt.Errorf("ReplicaSet identity unavailable")
		}
		old[rs.UID] = rs.Name
		if (rs.Spec.Replicas != nil && *rs.Spec.Replicas > 0) || rs.Status.Replicas > 0 {
			paths[fmt.Sprintf("rollout.replicasets[%s/%s]", rs.Namespace, rs.Name)] = true
		}
	}
	if len(old) > 0 {
		pods, err := w.Client.CoreV1().Pods(deployment.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			owner := metav1.GetControllerOf(pod)
			if owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" || old[owner.UID] == "" || old[owner.UID] != owner.Name {
				continue
			}
			paths[fmt.Sprintf("rollout.replicasets[%s/%s]", pod.Namespace, owner.Name)] = true
			paths[fmt.Sprintf("rollout.pods[%s/%s]", pod.Namespace, pod.Name)] = true
		}
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out, nil
}
