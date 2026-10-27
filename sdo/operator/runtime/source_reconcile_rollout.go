package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// Polling tool calls still give source-owned rollout progress one bounded
// window for the Deployment controller to publish its replacement.
const sourceReconcileProgressCeiling = 5 * time.Second

// ProgressSourceRollouts retires only unavailable old templates controlled by
// the exact reconciled Deployment. Ready capacity, foreign ReplicaSets, paused
// workloads and a rollout with no replacement remain the controller's to manage.
func (w KubernetesPlanRollouts) ProgressSourceRollouts(ctx context.Context, refs []sdk.ObjectRef, expected map[sdk.ObjectRef]types.UID) ([]*contracts.SourceRolloutStep, error) {
	if w.Client == nil {
		return nil, fmt.Errorf("Kubernetes rollout client is required")
	}
	var steps []*contracts.SourceRolloutStep
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return steps, err
		}
		if ref.APIVersion != "apps/v1" || ref.Kind != "Deployment" {
			continue
		}
		deployment, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return steps, err
		}
		if expected[ref] == "" || deployment.UID != expected[ref] {
			return steps, fmt.Errorf("reconciled Deployment identity changed or is unavailable")
		}
		if deployment.Spec.Paused || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas == 0 || deployment.Status.ObservedGeneration < deployment.Generation {
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
		// Require positive identity evidence for an old revision. Neither a
		// stale aggregate readiness count nor template inequality proves that.
		eligible := func(rs *appsv1.ReplicaSet) bool {
			if !owned(rs) || matches(rs) || rs.DeletionTimestamp != nil || rs.Spec.Replicas == nil || *rs.Spec.Replicas == 0 || rs.Status.ReadyReplicas > 0 {
				return false
			}
			hash := rs.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
			revision, parseErr := strconv.ParseInt(rs.Annotations["deployment.kubernetes.io/revision"], 10, 64)
			if hash == "" || parseErr != nil {
				return false
			}
			replacement := false
			for i := range sets.Items {
				other := &sets.Items[i]
				if !owned(other) {
					continue
				}
				otherRevision, err := strconv.ParseInt(other.Annotations["deployment.kubernetes.io/revision"], 10, 64)
				if err != nil {
					return false
				}
				if matches(other) && rs.CreationTimestamp.After(other.CreationTimestamp.Time) {
					return false
				}
				if matches(other) && other.DeletionTimestamp == nil && other.Spec.Replicas != nil && *other.Spec.Replicas > 0 && otherRevision > revision && other.Labels[appsv1.DefaultDeploymentUniqueLabelKey] != "" && other.Labels[appsv1.DefaultDeploymentUniqueLabelKey] != hash {
					replacement = true
				}
			}
			return replacement
		}
		protectedPods := func(rs *appsv1.ReplicaSet) (bool, error) {
			pods, err := w.Client.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for i := range pods.Items {
				pod := &pods.Items[i]
				owner := metav1.GetControllerOf(pod)
				if owner == nil || owner.UID != rs.UID || owner.Name != rs.Name || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" {
					continue
				}
				// An unexpected Pod hash is uncertain ownership evidence for
				// this revision; scaling its parent would delete it as well.
				if pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey] != rs.Labels[appsv1.DefaultDeploymentUniqueLabelKey] {
					return true, nil
				}
				for _, condition := range pod.Status.Conditions {
					if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
						return true, nil
					}
				}
			}
			return false, nil
		}
		for _, candidate := range sets.Items {
			rs := &candidate
			if !eligible(rs) {
				continue
			}
			ready, err := protectedPods(rs)
			if err != nil {
				return steps, err
			}
			if ready {
				continue
			}
			// Re-read the exact owner and candidate immediately before the
			// destructive write, then inspect Pod readiness independently of
			// the ReplicaSet controller's asynchronously published status.
			freshDeployment, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if err != nil {
				return steps, err
			}
			if freshDeployment.UID != deployment.UID || !equality.Semantic.DeepEqual(freshDeployment.Spec, deployment.Spec) || freshDeployment.Status.ObservedGeneration < freshDeployment.Generation {
				continue
			}
			freshSets, err := w.Client.AppsV1().ReplicaSets(ref.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return steps, err
			}
			sets = freshSets
			fresh, err := w.Client.AppsV1().ReplicaSets(rs.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
			if err != nil {
				return steps, err
			}
			if fresh.UID != rs.UID || !eligible(fresh) {
				continue
			}
			ready, err = protectedPods(fresh)
			if err != nil {
				return steps, err
			}
			if ready {
				continue
			}
			rs = fresh
			step := &contracts.SourceRolloutStep{Ref: planRefProto(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: rs.Namespace, Name: rs.Name}), Status: "unknown", Workload: planRefProto(ref)}
			steps = append(steps, step)
			if rs.UID == "" || rs.ResourceVersion == "" {
				step.Error = "stale ReplicaSet identity preconditions unavailable"
				return steps, fmt.Errorf("%s", step.Error)
			}
			if err := ctx.Err(); err != nil {
				step.Error = "rollout progress budget exhausted"
				return steps, err
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
	ProgressSourceRollouts(context.Context, []sdk.ObjectRef, map[sdk.ObjectRef]types.UID) ([]*contracts.SourceRolloutStep, error)
}

type sourceRolloutWaiter struct {
	sourceProgressRollouts
	refs     []sdk.ObjectRef
	expected map[sdk.ObjectRef]types.UID
	result   *PlanApplicationResult
}

func (w sourceRolloutWaiter) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	steps, err := w.ProgressSourceRollouts(ctx, w.refs, w.expected)
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
			expected := map[sdk.ObjectRef]types.UID{}
			for _, image := range result.PreImages {
				if image.Object != nil {
					expected[image.Ref] = image.Object.GetUID()
				}
			}
			// The source action owns this short step even for polling callers;
			// returning at the first observation can precede RS creation and
			// strand cleanup until another model turn. Never extend a shorter
			// rollout/stage/incident context.
			if e.PollRollouts {
				e.PollRollouts = false
				if e.RolloutTimeout <= 0 || e.RolloutTimeout > sourceReconcileProgressCeiling {
					e.RolloutTimeout = sourceReconcileProgressCeiling
				}
			}
			e.Rollouts = sourceRolloutWaiter{sourceProgressRollouts: progress, refs: reconciled, expected: expected, result: result}
		}
	}
	return e.waitForRollouts(ctx, incident, refs)
}
