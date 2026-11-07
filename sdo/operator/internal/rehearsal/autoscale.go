//go:build integration

package rehearsal

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// controllerManager is the field manager the real Deployment controller and
	// HorizontalPodAutoscaler controller write under.
	controllerManager = "kube-controller-manager"
	// RevisionAnnotation is the Deployment controller's rollout counter.
	RevisionAnnotation = "deployment.kubernetes.io/revision"
)

// recordRevision does what the Deployment controller does for every rollout:
// it numbers the new ReplicaSet in the Deployment's own annotations, under its
// own field manager. The write moves metadata.generation (a Deployment's
// annotation change does), exactly as on a real cluster, and the field is one
// SDO never applied.
func (k *Kubelet) recordRevision(ctx context.Context, d *appsv1.Deployment, rolledOut bool) {
	revision := int64(0)
	if current := d.Annotations[RevisionAnnotation]; current != "" {
		revision, _ = strconv.ParseInt(current, 10, 64)
	}
	if revision > 0 && !rolledOut {
		return
	}
	updated := d.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[RevisionAnnotation] = strconv.FormatInt(revision+1, 10)
	result, err := k.Client.AppsV1().Deployments(k.Namespace).Update(ctx, updated, metav1.UpdateOptions{FieldManager: controllerManager})
	k.fail(err)
	if err == nil {
		*d = *result
	}
}

// autoscale is the HorizontalPodAutoscaler controller: for every autoscaler
// in the namespace it scales the target through the scale subresource, under
// the controller manager's field manager. The modeled load grows by one
// replica per release (the baseline sits at minReplicas, capped by
// maxReplicas), so every release finds the target at a count other than the
// rendered one, as a live autoscaler leaves it.
func (k *Kubelet) autoscale(ctx context.Context) {
	hpas, err := k.Client.AutoscalingV2().HorizontalPodAutoscalers(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err != nil {
		return
	}
	if k.scaled == nil {
		k.scaled = map[string]bool{}
	}
	for _, hpa := range hpas.Items {
		minimum := int32(1)
		if hpa.Spec.MinReplicas != nil {
			minimum = *hpa.Spec.MinReplicas
		}
		desired := minimum + int32(k.release)
		if desired > hpa.Spec.MaxReplicas {
			desired = hpa.Spec.MaxReplicas
		}
		target := hpa.Spec.ScaleTargetRef
		var scale *autoscalingv1.Scale
		switch target.Kind {
		case "Deployment":
			scale, err = k.Client.AppsV1().Deployments(k.Namespace).GetScale(ctx, target.Name, metav1.GetOptions{})
		case "StatefulSet":
			scale, err = k.Client.AppsV1().StatefulSets(k.Namespace).GetScale(ctx, target.Name, metav1.GetOptions{})
		default:
			continue
		}
		k.fail(err)
		if err != nil {
			continue
		}
		// Something other than the autoscaler wrote the count after the
		// autoscaler settled it in this release: a deploy or adoption applied
		// the rendered replicas over the autoscaler's. Reported as a harness
		// error, which the rehearsal tests assert empty.
		key := fmt.Sprintf("%d/%s/%s", k.release, target.Kind, target.Name)
		if scale.Spec.Replicas != desired && k.scaled[key] {
			k.fail(fmt.Errorf("%s/%s: replicas reset to %d after the autoscaler set %d in release %d (a deploy or adoption applied the rendered count)", target.Kind, target.Name, scale.Spec.Replicas, desired, k.release))
		}
		if scale.Spec.Replicas == desired {
			k.scaled[key] = true
			continue
		}
		k.scaled[key] = true
		scale.Spec.Replicas = desired
		options := metav1.UpdateOptions{FieldManager: controllerManager}
		if target.Kind == "Deployment" {
			_, err = k.Client.AppsV1().Deployments(k.Namespace).UpdateScale(ctx, target.Name, scale, options)
		} else {
			_, err = k.Client.AppsV1().StatefulSets(k.Namespace).UpdateScale(ctx, target.Name, scale, options)
		}
		k.fail(err)
	}
}
