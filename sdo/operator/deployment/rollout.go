package deployment

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
)

// DeploymentRolloutComplete is shared by plan execution, deployment observation
// and lock publication. An available old replica cannot attest the new rollout.
func DeploymentRolloutComplete(o *appsv1.Deployment) bool {
	desired := int32(1)
	if o.Spec.Replicas != nil {
		desired = *o.Spec.Replicas
	}
	return o.Status.ObservedGeneration >= o.Generation && o.Status.UpdatedReplicas >= desired && o.Status.AvailableReplicas >= desired && o.Status.Replicas <= desired
}

// JobRolloutComplete respects the requested completions, including its default.
func JobRolloutComplete(o *batchv1.Job) bool {
	desired := int32(1)
	if o.Spec.Completions != nil {
		desired = *o.Spec.Completions
	}
	return desired > 0 && o.Status.Succeeded >= desired
}

// StatefulSetRolloutComplete mirrors kubectl rollout status: the controller
// observed the current generation, every desired replica is ready, and every
// replica outside a rolling-update partition runs the update revision. An
// OnDelete StatefulSet never replaces Pods itself, so only observation counts.
func StatefulSetRolloutComplete(o *appsv1.StatefulSet) bool {
	if o.Status.ObservedGeneration < o.Generation {
		return false
	}
	if o.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
		return true
	}
	desired := int32(1)
	if o.Spec.Replicas != nil {
		desired = *o.Spec.Replicas
	}
	if o.Status.ReadyReplicas < desired {
		return false
	}
	if rolling := o.Spec.UpdateStrategy.RollingUpdate; rolling != nil && rolling.Partition != nil && *rolling.Partition > 0 {
		return o.Status.UpdatedReplicas >= desired-*rolling.Partition
	}
	return o.Status.UpdatedReplicas >= desired && o.Status.UpdateRevision == o.Status.CurrentRevision
}

// DaemonSetRolloutComplete mirrors kubectl rollout status: the controller
// observed the current generation and every scheduled node runs an updated,
// available Pod. An OnDelete DaemonSet never replaces Pods itself.
func DaemonSetRolloutComplete(o *appsv1.DaemonSet) bool {
	if o.Status.ObservedGeneration < o.Generation {
		return false
	}
	if o.Spec.UpdateStrategy.Type == appsv1.OnDeleteDaemonSetStrategyType {
		return true
	}
	desired := o.Status.DesiredNumberScheduled
	return o.Status.UpdatedNumberScheduled >= desired && o.Status.NumberAvailable >= desired
}
