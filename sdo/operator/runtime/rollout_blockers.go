package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
)

func (w KubernetesPlanRollouts) rolloutBlocker(ctx context.Context, d *appsv1.Deployment) *contracts.RolloutBlocker {
	sets, setsErr := w.Client.AppsV1().ReplicaSets(d.Namespace).List(ctx, metav1.ListOptions{})
	pods, podsErr := w.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{})
	events, eventsErr := w.Client.CoreV1().Events(d.Namespace).List(ctx, metav1.ListOptions{FieldSelector: "type=Warning"})
	var rs []appsv1.ReplicaSet
	var ps []corev1.Pod
	var es []corev1.Event
	if setsErr == nil {
		rs = sets.Items
	}
	if podsErr == nil {
		ps = pods.Items
	}
	if eventsErr == nil {
		es = events.Items
	}
	observation := deploymentRolloutBlocker(d, rs, ps, es)
	blocker := observation.blocker
	for _, read := range []struct {
		name string
		err  error
	}{{"ReplicaSets", setsErr}, {"Pods", podsErr}, {"Events", eventsErr}} {
		if read.err != nil {
			blocker.WarningEvents = append(blocker.WarningEvents, boundedRolloutText(read.name+" evidence unavailable: "+read.err.Error(), 512))
		}
	}
	return blocker
}

type rolloutObservation struct {
	blocker *contracts.RolloutBlocker
	pending bool
	summary string
}

// deploymentRolloutBlocker projects one observation without I/O. The same
// ownership join serves write readback and controller closure verification.
func deploymentRolloutBlocker(d *appsv1.Deployment, sets []appsv1.ReplicaSet, pods []corev1.Pod, events []corev1.Event) rolloutObservation {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
	reasons := []string{fmt.Sprintf("%d/%d updated, %d ready, %d available; %d old template replicas; observed generation %d/%d", d.Status.UpdatedReplicas, deploymentDesiredReplicas(d), d.Status.ReadyReplicas, d.Status.AvailableReplicas, max(0, d.Status.Replicas-d.Status.UpdatedReplicas), d.Status.ObservedGeneration, d.Generation)}
	var failures []string
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue || c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			message := string(c.Type) + " " + c.Reason + ": " + c.Message
			reasons = append(reasons, message)
			failures = append(failures, message)
		}
	}
	uids := map[types.UID]bool{}
	if d.UID != "" {
		uids[d.UID] = true
	}
	oldSets := map[types.UID]bool{}
	for _, rs := range sets {
		if rs.Namespace != d.Namespace || !rolloutOwnedBy(rs.OwnerReferences, "Deployment", d.UID) || rs.UID == "" {
			continue
		}
		oldSets[rs.UID] = !sameRolloutTemplate(rs.Spec.Template, d.Spec.Template)
		if oldSets[rs.UID] || rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
			continue
		}
		uids[rs.UID] = true
		for _, c := range rs.Status.Conditions {
			if c.Type == appsv1.ReplicaSetReplicaFailure && c.Status == corev1.ConditionTrue {
				message := fmt.Sprintf("ReplicaSet/%s %s: %s", rs.Name, c.Reason, c.Message)
				reasons = append(reasons, message)
				failures = append(failures, message)
			}
		}
	}
	oldPodRemains := false
	podFailureRemains := false
	for _, pod := range pods {
		if pod.Namespace != d.Namespace || pod.UID == "" || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		owned, old := false, false
		for uid, isOld := range oldSets {
			if rolloutOwnedBy(pod.OwnerReferences, "ReplicaSet", uid) {
				owned, old = true, old || isOld
			}
		}
		if !owned {
			continue
		}
		uids[pod.UID] = true
		for _, owner := range pod.OwnerReferences {
			if _, ok := oldSets[owner.UID]; ok && rolloutOwnedBy([]metav1.OwnerReference{owner}, "ReplicaSet", owner.UID) {
				uids[owner.UID] = true
			}
		}
		if old {
			oldPodRemains = true
			reasons = append(reasons, "Pod/"+pod.Name+" still runs old template")
		}
		for _, c := range pod.Status.Conditions {
			if c.Status == corev1.ConditionFalse && c.Reason != "" {
				podFailureRemains = true
				reasons = append(reasons, fmt.Sprintf("Pod/%s %s: %s", pod.Name, c.Reason, c.Message))
			}
		}
		statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
		for _, status := range statuses {
			if waiting := status.State.Waiting; waiting != nil {
				podFailureRemains = true
				reasons = append(reasons, fmt.Sprintf("Pod/%s container/%s %s: %s", pod.Name, status.Name, waiting.Reason, waiting.Message))
			}
		}
	}
	// Sort diagnostics for stable replay, keeping the progress counters first.
	sort.Strings(reasons[1:])
	blocker := &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: boundedRolloutText(strings.Join(reasons, "; "), 4096)}
	for _, event := range events {
		if event.Type == corev1.EventTypeWarning && event.Namespace == d.Namespace && event.InvolvedObject.Namespace == d.Namespace && uids[event.InvolvedObject.UID] {
			blocker.WarningEvents = append(blocker.WarningEvents, boundedRolloutText(event.InvolvedObject.Kind+"/"+event.InvolvedObject.Name+" "+event.Reason+": "+event.Message, 512))
		}
	}
	sort.Strings(blocker.WarningEvents)
	if len(blocker.WarningEvents) > 4 {
		blocker.WarningEvents = blocker.WarningEvents[:4]
	}
	summary := reasons[0]
	if len(failures) > 0 {
		sort.Strings(failures)
		summary = failures[0]
	} else if len(blocker.WarningEvents) > 0 {
		summary = blocker.WarningEvents[0]
	} else if len(reasons) > 1 {
		summary = reasons[1]
	}
	return rolloutObservation{blocker: blocker, pending: oldPodRemains || podFailureRemains || !deployment.DeploymentRolloutComplete(d), summary: boundedRolloutText(summary, 200)}
}

func rolloutOwnedBy(owners []metav1.OwnerReference, kind string, uid types.UID) bool {
	if uid == "" {
		return false
	}
	for _, owner := range owners {
		if owner.Kind == kind && owner.UID == uid && (owner.Controller == nil || *owner.Controller) {
			return true
		}
	}
	return false
}

func sameRolloutTemplate(left, right corev1.PodTemplateSpec) bool {
	left, right = *left.DeepCopy(), *right.DeepCopy()
	delete(left.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	delete(right.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return equality.Semantic.DeepEqual(left, right)
}

func deploymentDesiredReplicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}
func boundedRolloutText(value string, limit int) string {
	if len(value) > limit {
		return value[:limit] + " [truncated]"
	}
	return value
}
