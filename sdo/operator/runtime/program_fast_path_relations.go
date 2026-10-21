package runtime

import (
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
)

type scalingPolicySnapshot interface {
	HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	PodDisruptionBudgets() []policyv1.PodDisruptionBudget
}

func fastPathDirectlyRelated(view sdk.DetectionContext, subject, bound sdk.ObjectRef) bool {
	if subject.Namespace == "" || subject.Namespace != bound.Namespace || bound.Name == "" {
		return false
	}
	var labels map[string]string
	found := false
	switch bound.Kind {
	case "Deployment":
		for _, deployment := range view.Deployments() {
			if deployment.Namespace == bound.Namespace && deployment.Name == bound.Name {
				labels = deployment.Spec.Template.Labels
				found = true
				break
			}
		}
	case "StatefulSet":
		if snapshot, ok := view.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
			for _, statefulSet := range snapshot.StatefulSets() {
				if statefulSet.Namespace == bound.Namespace && statefulSet.Name == bound.Name {
					labels = statefulSet.Spec.Template.Labels
					found = true
					break
				}
			}
		}
	default:
		return false
	}
	if !found {
		return false
	}
	switch subject.Kind {
	case "Service":
		for _, service := range view.Services() {
			if service.Namespace == subject.Namespace && service.Name == subject.Name && len(service.Spec.Selector) > 0 && labelsContain(labels, service.Spec.Selector) {
				return true
			}
		}
	case "ReplicaSet":
		for _, replicaSet := range view.ReplicaSets() {
			if replicaSet.Namespace == subject.Namespace && replicaSet.Name == subject.Name && ownedBy(replicaSet.OwnerReferences, bound) {
				return true
			}
		}
	case "Pod":
		for _, pod := range view.Pods() {
			if pod.Namespace != subject.Namespace || pod.Name != subject.Name {
				continue
			}
			if ownedBy(pod.OwnerReferences, bound) {
				return true
			}
			if bound.Kind == "Deployment" {
				for _, replicaSet := range view.ReplicaSets() {
					if replicaSet.Namespace == bound.Namespace && ownedBy(replicaSet.OwnerReferences, bound) && ownedBy(pod.OwnerReferences, sdk.ObjectRef{Kind: "ReplicaSet", Namespace: bound.Namespace, Name: replicaSet.Name}) {
						return true
					}
				}
			}
		}
	case "HorizontalPodAutoscaler", "PodDisruptionBudget":
		snapshot, ok := view.(scalingPolicySnapshot)
		if !ok {
			return false
		}
		if subject.Kind == "HorizontalPodAutoscaler" {
			for _, hpa := range snapshot.HorizontalPodAutoscalers() {
				if hpa.Namespace == subject.Namespace && hpa.Name == subject.Name && hpa.Spec.ScaleTargetRef.Kind == bound.Kind && hpa.Spec.ScaleTargetRef.Name == bound.Name && (hpa.Spec.ScaleTargetRef.APIVersion == "" || bound.APIVersion == "" || hpa.Spec.ScaleTargetRef.APIVersion == bound.APIVersion) {
					return true
				}
			}
		} else {
			for _, pdb := range snapshot.PodDisruptionBudgets() {
				if pdb.Namespace != subject.Namespace || pdb.Name != subject.Name || pdb.Spec.Selector == nil {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err == nil && selector.Matches(klabels.Set(labels)) {
					return true
				}
			}
		}
	}
	return false
}

func labelsContain(labels, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func ownedBy(owners []metav1.OwnerReference, bound sdk.ObjectRef) bool {
	for _, owner := range owners {
		if owner.Kind == bound.Kind && owner.Name == bound.Name && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}
