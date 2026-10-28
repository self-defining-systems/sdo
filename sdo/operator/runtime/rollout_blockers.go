package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
)

func (w KubernetesPlanRollouts) rolloutBlocker(ctx context.Context, d *appsv1.Deployment, started time.Time) (*contracts.RolloutBlocker, bool, *AdmissionBlockedError) {
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
	observation := deploymentRolloutBlocker(d, rs, ps, es, started)
	blocker := observation.blocker
	for _, read := range []struct {
		name string
		err  error
	}{{"ReplicaSets", setsErr}, {"Pods", podsErr}, {"Events", eventsErr}} {
		if read.err != nil {
			blocker.WarningEvents = append(blocker.WarningEvents, boundedRolloutText(read.name+" evidence unavailable: "+read.err.Error(), 512))
		}
	}
	return blocker, observation.stalled, w.admissionBlocked(ctx, blocker, observation.admission)
}

type rolloutObservation struct {
	admission []string
	blocker   *contracts.RolloutBlocker
	pending   bool
	// stalled means the rollout cannot progress until another actor changes the
	// cluster (removes a holder, adds capacity, edits the template). Waiting
	// only observes, so it can never clear this.
	stalled bool
	summary string
}

// deploymentRolloutBlocker projects one observation without I/O. The same
// ownership join serves write readback and controller closure verification.
func deploymentRolloutBlocker(d *appsv1.Deployment, sets []appsv1.ReplicaSet, pods []corev1.Pod, events []corev1.Event, since ...time.Time) rolloutObservation {
	started := time.Time{}
	if len(since) > 0 {
		started = since[0]
	}
	currentEvidence := func(at time.Time, observed, generation int64) bool {
		// Child controllers may still report a fresh failure from the previous
		// attempt until the Deployment controller observes this rollout generation.
		return d.Status.ObservedGeneration >= d.Generation && observed >= generation && !at.IsZero() && !at.Before(started)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
	reasons := []string{fmt.Sprintf("%d/%d updated, %d ready, %d available; %d old template replicas; observed generation %d/%d", d.Status.UpdatedReplicas, deploymentDesiredReplicas(d), d.Status.ReadyReplicas, d.Status.AvailableReplicas, max(0, d.Status.Replicas-d.Status.UpdatedReplicas), d.Status.ObservedGeneration, d.Generation)}
	var failures []string
	var admission []string
	var stuck []string
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded" && d.Status.ObservedGeneration >= d.Generation {
			stuck = append(stuck, "Progressing "+c.Reason+": "+c.Message)
		}
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue || c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			message := string(c.Type) + " " + c.Reason + ": " + c.Message
			reasons = append(reasons, message)
			failures = append(failures, message)
			if c.Reason == "FailedCreate" && admissionRejection(c.Message) && currentEvidence(c.LastUpdateTime.Time, d.Status.ObservedGeneration, d.Generation) {
				admission = append(admission, message)
			}
		}
	}
	uids := map[types.UID]bool{}
	admissionUIDs := map[types.UID]bool{}
	if d.UID != "" {
		uids[d.UID] = true
		admissionUIDs[d.UID] = d.Status.ObservedGeneration >= d.Generation && d.Status.UpdatedReplicas < deploymentDesiredReplicas(d)
	}
	oldSets := map[types.UID]bool{}
	setNames := map[types.UID]string{}
	for _, rs := range sets {
		if rs.Namespace != d.Namespace || !rolloutOwnedBy(rs.OwnerReferences, "Deployment", d.UID) || rs.UID == "" {
			continue
		}
		oldSets[rs.UID] = !sameRolloutTemplate(rs.Spec.Template, d.Spec.Template)
		setNames[rs.UID] = rs.Name
		if oldSets[rs.UID] || rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
			continue
		}
		uids[rs.UID] = true
		desired := int32(1)
		if rs.Spec.Replicas != nil {
			desired = *rs.Spec.Replicas
		}
		admissionUIDs[rs.UID] = rs.Status.Replicas < desired && rs.Status.ObservedGeneration >= rs.Generation
		for _, c := range rs.Status.Conditions {
			if c.Type == appsv1.ReplicaSetReplicaFailure && c.Status == corev1.ConditionTrue {
				message := fmt.Sprintf("ReplicaSet/%s %s: %s", rs.Name, c.Reason, c.Message)
				reasons = append(reasons, message)
				failures = append(failures, message)
				if c.Reason == "FailedCreate" && admissionRejection(c.Message) && admissionUIDs[rs.UID] && currentEvidence(c.LastTransitionTime.Time, rs.Status.ObservedGeneration, rs.Generation) {
					admission = append(admission, message)
				}
			}
		}
	}
	oldPodRemains := false
	podFailureRemains := false
	// A terminating Pod in the namespace can still free the capacity or
	// placement a replacement needs, so no stall is claimed while one drains.
	draining := false
	var holders []string
	for _, pod := range pods {
		if pod.Namespace == d.Namespace && pod.DeletionTimestamp != nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			draining = true
		}
		if pod.Namespace != d.Namespace || pod.UID == "" || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		owned, old := false, false
		holder := ""
		for uid, isOld := range oldSets {
			if rolloutOwnedBy(pod.OwnerReferences, "ReplicaSet", uid) {
				owned, old = true, old || isOld
				if isOld {
					holder = "Pod/" + pod.Name + " of ReplicaSet/" + setNames[uid]
				}
			}
		}
		if !owned {
			continue
		}
		uids[pod.UID] = true
		if !old {
			admissionUIDs[pod.UID] = true
		}
		for _, owner := range pod.OwnerReferences {
			if _, ok := oldSets[owner.UID]; ok && rolloutOwnedBy([]metav1.OwnerReference{owner}, "ReplicaSet", owner.UID) {
				uids[owner.UID] = true
			}
		}
		if old {
			oldPodRemains = true
			holders = append(holders, holder)
			reasons = append(reasons, "Pod/"+pod.Name+" still runs old template")
		}
		for _, c := range pod.Status.Conditions {
			if !old && c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
				stuck = append(stuck, fmt.Sprintf("replacement Pod/%s %s: %s", pod.Name, c.Reason, c.Message))
			}
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
	pending := oldPodRemains || podFailureRemains || !deployment.DeploymentRolloutComplete(d)
	stalled := pending && len(stuck) > 0 && !draining
	cause := ""
	if stalled {
		// Name the cause and every old-template holder first, so even a
		// compacted receipt tells the next actor what to change.
		sort.Strings(stuck)
		sort.Strings(holders)
		cause = "cannot progress without another actor: " + strings.Join(stuck, "; ")
		if len(holders) > 0 {
			cause += "; old-template " + strings.Join(holders, ", ") + " still holds its place"
		} else {
			cause += "; no old-template Pod is draining"
		}
		reasons = append([]string{cause}, reasons...)
	}
	blocker := &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: boundedRolloutText(strings.Join(reasons, "; "), 4096)}
	for _, event := range events {
		if event.Type == corev1.EventTypeWarning && event.Namespace == d.Namespace && event.InvolvedObject.Namespace == d.Namespace && uids[event.InvolvedObject.UID] {
			at := event.LastTimestamp.Time
			if event.EventTime.Time.After(at) {
				at = event.EventTime.Time
			}
			if event.Series != nil && event.Series.LastObservedTime.Time.After(at) {
				at = event.Series.LastObservedTime.Time
			}
			if admissionUIDs[event.InvolvedObject.UID] && event.Reason == "FailedCreate" && admissionRejection(event.Message) && currentEvidence(at, d.Status.ObservedGeneration, d.Generation) {
				admission = append(admission, event.InvolvedObject.Kind+"/"+event.InvolvedObject.Name+": "+event.Message)
			}
			blocker.WarningEvents = append(blocker.WarningEvents, boundedRolloutText(event.InvolvedObject.Kind+"/"+event.InvolvedObject.Name+" "+event.Reason+": "+event.Message, 512))
		}
	}
	sort.Strings(blocker.WarningEvents)
	if len(blocker.WarningEvents) > 4 {
		blocker.WarningEvents = blocker.WarningEvents[:4]
	}
	summary := reasons[0]
	if stalled {
		summary = cause
	} else if len(failures) > 0 {
		sort.Strings(failures)
		summary = failures[0]
	} else if len(blocker.WarningEvents) > 0 {
		summary = blocker.WarningEvents[0]
	} else if len(reasons) > 1 {
		summary = reasons[1]
	}
	return rolloutObservation{admission: admission, blocker: blocker, pending: pending, stalled: stalled, summary: boundedRolloutText(summary, 200)}
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
