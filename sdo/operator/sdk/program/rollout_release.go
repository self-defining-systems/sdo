package program

import (
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

// RetireOldRevision scales one old-revision ReplicaSet of a Deployment to zero.
// It releases a rollout whose replacement Pod cannot be placed while the old
// revision's Pod holds its place (hard anti-affinity, a host port or capacity
// on a small cluster). The Deployment controller never scales a retired
// revision back up. It is refused like any ReplicaSet scale for a current
// revision or a ReplicaSet without an observed controlling Deployment.
type RetireOldRevision struct {
	Ref sdk.ObjectRef
}

func (RetireOldRevision) action() {}

// RolloutView is the observation a stalled-rollout release is chosen from.
// ReadView and detector snapshots satisfy it.
type RolloutView interface {
	Deployments() []appsv1.Deployment
	ReplicaSets() []appsv1.ReplicaSet
	Pods() []corev1.Pod
}

// StalledRolloutRelease returns the standard release of a Deployment rollout
// whose current-revision Pod is Unschedulable: one RetireOldRevision for each
// old revision that still keeps a replica, in name order. It returns nothing
// while no current-revision Pod is Unschedulable, so a crashing or merely slow
// rollout never loses the revision that still serves. SDO offers the same
// actions to the responder when it hands off a stalled rollout.
func StalledRolloutRelease(view RolloutView, deployment sdk.ObjectRef) []Action {
	var owner *appsv1.Deployment
	for i, candidate := range view.Deployments() {
		if candidate.Namespace == deployment.Namespace && candidate.Name == deployment.Name {
			owner = &view.Deployments()[i]
			break
		}
	}
	if owner == nil || owner.Annotations[deploymentRevisionAnnotation] == "" {
		return nil
	}
	current := owner.Annotations[deploymentRevisionAnnotation]
	currentSets := map[string]bool{}
	var old []string
	for _, set := range view.ReplicaSets() {
		if set.Namespace != owner.Namespace || !controlledBy(set.OwnerReferences, "Deployment", owner.Name, string(owner.UID)) {
			continue
		}
		revision := set.Annotations[deploymentRevisionAnnotation]
		switch {
		case revision == current:
			currentSets[set.Name] = true
		case revision != "" && (set.Spec.Replicas == nil || *set.Spec.Replicas > 0 || set.Status.Replicas > 0):
			old = append(old, set.Name)
		}
	}
	unplaceable := false
	for _, pod := range view.Pods() {
		if pod.Namespace != owner.Namespace || pod.DeletionTimestamp != nil || !ownedByAny(pod.OwnerReferences, currentSets) {
			continue
		}
		for _, condition := range pod.Status.Conditions {
			unplaceable = unplaceable || condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable
		}
	}
	if !unplaceable {
		return nil
	}
	sort.Strings(old)
	actions := make([]Action, 0, len(old))
	for _, name := range old {
		actions = append(actions, RetireOldRevision{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: owner.Namespace, Name: name}})
	}
	return actions
}

func controlledBy(owners []metav1.OwnerReference, kind, name, uid string) bool {
	for _, owner := range owners {
		if owner.Controller != nil && *owner.Controller && owner.Kind == kind && owner.Name == name && (uid == "" || owner.UID == "" || string(owner.UID) == uid) {
			return true
		}
	}
	return false
}

func ownedByAny(owners []metav1.OwnerReference, sets map[string]bool) bool {
	for _, owner := range owners {
		if owner.Controller != nil && *owner.Controller && owner.Kind == "ReplicaSet" && sets[owner.Name] {
			return true
		}
	}
	return false
}
