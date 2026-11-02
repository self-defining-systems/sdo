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
