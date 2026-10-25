package runtime

import (
	"context"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sort"
	"strings"
)

func (w KubernetesPlanRollouts) rolloutBlocker(ctx context.Context, d *appsv1.Deployment) *contracts.RolloutBlocker {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
	reasons := []string{fmt.Sprintf("%d/%d updated, %d ready, %d available replicas", d.Status.UpdatedReplicas, deploymentDesiredReplicas(d), d.Status.ReadyReplicas, d.Status.AvailableReplicas)}
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue || c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			reasons = append(reasons, string(c.Type)+" "+c.Reason+": "+c.Message)
		}
	}
	blocker := &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: boundedRolloutText(strings.Join(reasons, "; "), 1024)}
	uids := map[types.UID]bool{}
	if d.UID != "" {
		uids[d.UID] = true
	}
	sets, err := w.Client.AppsV1().ReplicaSets(d.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, rs := range sets.Items {
			for _, owner := range rs.OwnerReferences {
				if owner.Kind == "Deployment" && owner.UID == d.UID && d.UID != "" && rs.UID != "" {
					uids[rs.UID] = true
				}
			}
		}
	}
	events, err := w.Client.CoreV1().Events(d.Namespace).List(ctx, metav1.ListOptions{FieldSelector: "type=Warning"})
	if err == nil {
		for _, event := range events.Items {
			if event.Type == corev1.EventTypeWarning && event.InvolvedObject.Namespace == d.Namespace && uids[event.InvolvedObject.UID] {
				blocker.WarningEvents = append(blocker.WarningEvents, boundedRolloutText(event.Reason+": "+event.Message, 512))
			}
		}
	}
	sort.Strings(blocker.WarningEvents)
	if len(blocker.WarningEvents) > 4 {
		blocker.WarningEvents = blocker.WarningEvents[:4]
	}
	return blocker
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
