package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// ProgressSourceRollouts retires only unavailable old templates controlled by
// the exact reconciled Deployment. Ready capacity, foreign ReplicaSets, paused
// workloads and a rollout with no replacement remain the controller's to manage.
func (w KubernetesPlanRollouts) ProgressSourceRollouts(ctx context.Context, refs []sdk.ObjectRef) ([]*contracts.SourceRolloutStep, error) {
	var steps []*contracts.SourceRolloutStep
	for _, ref := range refs {
		if ref.APIVersion != "apps/v1" || ref.Kind != "Deployment" {
			continue
		}
		deployment, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return steps, err
		}
		if deployment.UID == "" || deployment.Spec.Paused || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas == 0 || deployment.Status.ObservedGeneration < deployment.Generation {
			continue
		}
		sets, err := w.Client.AppsV1().ReplicaSets(ref.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return steps, err
		}
		owned := func(rs *appsv1.ReplicaSet) bool {
			owner := metav1.GetControllerOf(rs)
			return owner != nil && owner.UID == deployment.UID && owner.APIVersion == "apps/v1" && owner.Kind == "Deployment" && owner.Name == deployment.Name
		}
		matches := func(rs *appsv1.ReplicaSet) bool {
			left, right := rs.Spec.Template.DeepCopy(), deployment.Spec.Template.DeepCopy()
			delete(left.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
			delete(right.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
			return equality.Semantic.DeepEqual(left, right)
		}
		replacement := false
		for i := range sets.Items {
			rs := &sets.Items[i]
			if owned(rs) && matches(rs) && rs.DeletionTimestamp == nil && rs.Spec.Replicas != nil && *rs.Spec.Replicas > 0 {
				replacement = true
			}
		}
		if !replacement {
			continue
		}
		for i := range sets.Items {
			rs := &sets.Items[i]
			if !owned(rs) || matches(rs) || rs.DeletionTimestamp != nil || rs.Spec.Replicas == nil || *rs.Spec.Replicas == 0 || rs.Status.ReadyReplicas > 0 {
				continue
			}
			step := &contracts.SourceRolloutStep{Ref: planRefProto(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: rs.Namespace, Name: rs.Name}), Status: "unknown", Workload: planRefProto(ref)}
			steps = append(steps, step)
			if rs.UID == "" || rs.ResourceVersion == "" {
				step.Error = "stale ReplicaSet identity preconditions unavailable"
				return steps, fmt.Errorf("%s", step.Error)
			}
			patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": rs.UID, "resourceVersion": rs.ResourceVersion}, "spec": map[string]any{"replicas": int64(0)}})
			_, err = w.Client.AppsV1().ReplicaSets(rs.Namespace).Patch(ctx, rs.Name, types.MergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				step.Error = "stale ReplicaSet scale failed"
				return steps, fmt.Errorf("scale stale ReplicaSet %s: %w", rs.Name, err)
			}
			live, readErr := w.Client.AppsV1().ReplicaSets(rs.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
			if readErr != nil || live.UID != rs.UID || live.Spec.Replicas == nil || *live.Spec.Replicas != 0 {
				step.Error = "stale ReplicaSet scale readback unconfirmed"
				return steps, fmt.Errorf("%s", step.Error)
			}
			step.Status = "scaled_to_zero"
		}
	}
	return steps, nil
}

type sourceProgressRollouts interface {
	Observe(context.Context, []sdk.ObjectRef) error
	ProgressSourceRollouts(context.Context, []sdk.ObjectRef) ([]*contracts.SourceRolloutStep, error)
}

type sourceRolloutWaiter struct {
	sourceProgressRollouts
	refs   []sdk.ObjectRef
	result *PlanApplicationResult
}

func (w sourceRolloutWaiter) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	steps, err := w.ProgressSourceRollouts(ctx, w.refs)
	for _, ref := range w.refs {
		for index, action := range w.result.Plan.Actions {
			target, _ := planActionRef(action)
			if !isSourceReconcile(action) || target != ref || index >= len(w.result.ActionReports) {
				continue
			}
			report := w.result.ActionReports[index].SourceReconcile
			if report == nil {
				report = &contracts.SourceReconcileResult{Ref: planRefProto(ref)}
				w.result.ActionReports[index].SourceReconcile = report
			}
			for _, step := range steps {
				// Progress returns only this namespace's exact owner-bound effects. The
				// parent identity is included in each step for multi-workload plans.
				if step.GetWorkload().GetName() == ref.Name && step.GetWorkload().GetNamespace() == ref.Namespace {
					report.RolloutSteps = append(report.RolloutSteps, step)
				}
			}
			if err != nil {
				report.ObservationError = "rollout progress observation failed"
			}
		}
	}
	if err != nil {
		return err
	}
	return w.sourceProgressRollouts.Observe(ctx, refs)
}
func (w sourceRolloutWaiter) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		err := w.Observe(ctx, refs)
		if err == nil {
			return nil
		}
		if _, pending := err.(*rolloutBlockedError); !pending {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}
}

// Both progress and the ordinary wait spend the existing rollout/stage context.
func (e PlanExecutor) waitForSourceRollouts(ctx context.Context, incident IncidentRequest, refs []sdk.ObjectRef, result *PlanApplicationResult) (*contracts.RolloutPending, error) {
	if progress, ok := e.Rollouts.(sourceProgressRollouts); ok {
		var reconciled []sdk.ObjectRef
		for _, action := range result.Plan.Actions {
			if isSourceReconcile(action) {
				ref, _ := planActionRef(action)
				reconciled = append(reconciled, ref)
			}
		}
		if len(reconciled) > 0 {
			e.Rollouts = sourceRolloutWaiter{sourceProgressRollouts: progress, refs: reconciled, result: result}
		}
	}
	return e.waitForRollouts(ctx, incident, refs)
}
