package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// planPodLister lists one namespace's Pods, so a restart can find the Pods its
// workload controller will not replace.
type planPodLister interface {
	NamespacePods(context.Context, string) ([]corev1.Pod, error)
}

func (c TypedPlanClient) NamespacePods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	if err := c.checkPlanNamespace(namespace); err != nil {
		return nil, err
	}
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	pods, err := c.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// StuckRestartPod is a Pod of a restarted workload that its controller will not
// replace after the template change, and why.
type StuckRestartPod struct {
	Pod    corev1.Pod
	Reason string
	// A source restore selects Pods by their current revision and availability.
	requireSameVersion bool
}

// PodsControllerWillNotRoll returns the workload's current Pods that its
// controller will not replace on a template change: every Pod under an
// OnDelete update strategy (StatefulSet, DaemonSet), and the unready Pods of
// an OrderedReady StatefulSet rolling update, which waits for every Pod to be
// Ready before it replaces any. Deployments and the other strategies replace
// their Pods themselves. Pods below a rolling-update partition stay by design.
func PodsControllerWillNotRoll(workload *unstructured.Unstructured, pods []corev1.Pod) ([]StuckRestartPod, error) {
	var owned []corev1.Pod
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if pod.DeletionTimestamp != nil || owner == nil || owner.Kind != workload.GetKind() || owner.Name != workload.GetName() || pod.Namespace != workload.GetNamespace() {
			continue
		}
		if workload.GetUID() != "" && owner.UID != workload.GetUID() {
			continue
		}
		owned = append(owned, pod)
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].Name < owned[j].Name })
	var stuck []StuckRestartPod
	switch workload.GetKind() {
	case "StatefulSet":
		var set appsv1.StatefulSet
		if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(workload.Object, &set); err != nil {
			return nil, err
		}
		if set.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType {
			for _, pod := range owned {
				stuck = append(stuck, StuckRestartPod{Pod: pod, Reason: "StatefulSet update strategy is OnDelete: its controller never replaces a Pod on a template change"})
			}
			return stuck, nil
		}
		if set.Spec.PodManagementPolicy == appsv1.ParallelPodManagement {
			return nil, nil
		}
		partition := 0
		if rolling := set.Spec.UpdateStrategy.RollingUpdate; rolling != nil && rolling.Partition != nil {
			partition = int(*rolling.Partition)
		}
		for _, pod := range owned {
			ordinal, err := strconv.Atoi(strings.TrimPrefix(pod.Name, set.Name+"-"))
			if podRunningAndReady(pod) || (err == nil && ordinal < partition) {
				continue
			}
			stuck = append(stuck, StuckRestartPod{Pod: pod, Reason: "Pod is not Ready: an OrderedReady StatefulSet rolling update waits for every Pod to be Ready, so its controller would never replace it"})
		}
	case "DaemonSet":
		var set appsv1.DaemonSet
		if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(workload.Object, &set); err != nil {
			return nil, err
		}
		if set.Spec.UpdateStrategy.Type == appsv1.OnDeleteDaemonSetStrategyType {
			for _, pod := range owned {
				stuck = append(stuck, StuckRestartPod{Pod: pod, Reason: "DaemonSet update strategy is OnDelete: its controller never replaces a Pod on a template change"})
			}
		}
	}
	return stuck, nil
}

func podRunningAndReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func restartRolloutRef(action program.Action) (sdk.ObjectRef, bool) {
	switch a := action.(type) {
	case program.RestartRollout:
		return a.Ref, true
	case *program.RestartRollout:
		if a != nil {
			return a.Ref, true
		}
	}
	return sdk.ObjectRef{}, false
}

// stuckRestartPods reads, before the restart lands, the Pods each restarted
// workload's controller will not replace. Only the Pods that exist before the
// template change are old-revision Pods.
func (e PlanExecutor) stuckRestartPods(ctx context.Context, plan program.Plan, workloads map[sdk.ObjectRef]*unstructured.Unstructured) ([]StuckRestartPod, error) {
	var stuck []StuckRestartPod
	seen := map[sdk.ObjectRef]bool{}
	for _, action := range plan.Actions {
		ref, ok := restartRolloutRef(action)
		if !ok || seen[ref] || (ref.Kind != "StatefulSet" && ref.Kind != "DaemonSet") {
			continue
		}
		seen[ref] = true
		lister, ok := e.Client.(planPodLister)
		if !ok {
			return nil, fmt.Errorf("RestartRollout of a %s requires a Pod list client", ref.Kind)
		}
		pods, err := lister.NamespacePods(ctx, ref.Namespace)
		if err != nil {
			return nil, fmt.Errorf("list Pods of %s/%s: %w", ref.Kind, ref.Name, err)
		}
		found, err := PodsControllerWillNotRoll(workloads[ref], pods)
		if err != nil {
			return nil, err
		}
		stuck = append(stuck, found...)
	}
	return stuck, nil
}

// replaceStuckPods deletes the old-revision Pods a restarted workload's
// controller will not replace, as an operator does after kubectl rollout
// restart, and records each delete and its reason. A Pod already gone or
// replaced needs nothing.
func (e PlanExecutor) replaceStuckPods(ctx context.Context, stuck []StuckRestartPod, result *PlanApplicationResult) error {
	if len(stuck) == 0 {
		return nil
	}
	deleter, ok := e.Client.(planPodDeleter)
	if !ok {
		return fmt.Errorf("RestartRollout requires a Pod delete client to replace Pods its controller will not roll")
	}
	for _, item := range stuck {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: item.Pod.Namespace, Name: item.Pod.Name}
		live, err := e.Client.Get(ctx, ref)
		if apierrors.IsNotFound(err) || (err == nil && (live == nil || live.GetUID() != item.Pod.UID || live.GetDeletionTimestamp() != nil)) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read Pod %s the restart must replace: %w", ref.Name, err)
		}
		if item.requireSameVersion && live.GetResourceVersion() != item.Pod.ResourceVersion {
			continue
		}
		if err := deleter.Delete(ctx, ref, item.Pod.UID, live.GetResourceVersion()); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete Pod %s the restart must replace (%s): %w", ref.Name, item.Reason, err)
		}
		after, getErr := e.Client.Get(ctx, ref)
		confirmed := apierrors.IsNotFound(getErr) || (getErr == nil && after != nil && (after.GetUID() != item.Pod.UID || after.GetDeletionTimestamp() != nil))
		state := "applied"
		if !confirmed {
			state = "unknown"
		}
		result.ObjectEffects = append(result.ObjectEffects, PlanObjectEffect{Operation: "delete", Ref: ref, UID: string(item.Pod.UID), State: state, Confirmed: confirmed, Irreversible: true, Reason: item.Reason})
	}
	return nil
}

// sourceRestoredStatefulSets identifies actual template changes made by
// accepted-source reconciliation. Other restores do not own a Pod rollout.
func sourceRestoredStatefulSets(result PlanApplicationResult) []PlanPatch {
	var restores []PlanPatch
	for _, patch := range result.Patches {
		if patch.Ref.Kind != "StatefulSet" || patch.Before == nil || patch.After == nil ||
			reflect.DeepEqual(statefulSetTemplate(patch.Before), statefulSetTemplate(patch.After)) {
			continue
		}
		for _, action := range result.ActionsApplied {
			if ref, err := planActionRef(action); err == nil && ref == patch.Ref && isSourceReconcile(action) {
				restores = append(restores, patch)
				break
			}
		}
	}
	return restores
}

func statefulSetTemplate(set *unstructured.Unstructured) any {
	template, _, _ := unstructured.NestedFieldNoCopy(set.Object, "spec", "template")
	return template
}

// advanceRestoredStatefulSets takes one live sample and replaces at most one
// Pod. The original apply and subsequent controller steps share this path.
func (e PlanExecutor) advanceRestoredStatefulSets(ctx context.Context, restores []PlanPatch, result *PlanApplicationResult, replace func(StuckRestartPod) error) (bool, error) {
	lister, ok := e.Client.(planPodLister)
	if !ok {
		return false, nil
	}
	var waiting bool
	for _, restore := range restores {
		live, err := e.Client.Get(ctx, restore.Ref)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return true, fmt.Errorf("read restored StatefulSet %s: %w", restore.Ref.Name, err)
		}
		if live == nil || live.GetUID() != restore.Before.GetUID() ||
			!reflect.DeepEqual(statefulSetTemplate(live), statefulSetTemplate(restore.After)) {
			continue // The original write no longer owns this template.
		}
		var set appsv1.StatefulSet
		if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &set); err != nil {
			return true, err
		}
		if set.Status.ObservedGeneration < set.Generation || set.Status.UpdateRevision == "" {
			waiting = true
			continue
		}
		pods, err := lister.NamespacePods(ctx, set.Namespace)
		if err != nil {
			return true, fmt.Errorf("list Pods of restored StatefulSet %s: %w", set.Name, err)
		}
		candidate, pending := nextRestoredStatefulSetPod(set, pods, result.ObjectEffects, e.clock()())
		if candidate != nil {
			return true, replace(*candidate)
		}
		waiting = waiting || pending
	}
	return waiting, nil
}

// Kubernetes owns RollingUpdate of a healthy old Pod. Help only an old,
// unavailable Pod that can hold that rollout, or an OnDelete template that
// requires explicit replacement. Work down the eligible ordinals.
func nextRestoredStatefulSetPod(set appsv1.StatefulSet, pods []corev1.Pod, effects []PlanObjectEffect, now time.Time) (*StuckRestartPod, bool) {
	start, replicas, partition := 0, 1, 0
	if set.Spec.Ordinals != nil {
		start = int(set.Spec.Ordinals.Start)
	}
	if set.Spec.Replicas != nil {
		replicas = int(*set.Spec.Replicas)
	}
	if rolling := set.Spec.UpdateStrategy.RollingUpdate; rolling != nil && rolling.Partition != nil {
		partition = int(*rolling.Partition)
	}
	onDelete := set.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType
	owned := make(map[string]corev1.Pod)
	highest := -1
	var next *StuckRestartPod
	allAvailable, terminating := true, false
	update := set.Status.UpdateRevision
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "StatefulSet" || owner.Name != set.Name || owner.UID != set.UID || pod.Namespace != set.Namespace {
			continue
		}
		ordinal, valid := statefulSetPodOrdinal(set.Name, pod.Name)
		if !valid || ordinal < start || ordinal >= start+replicas {
			continue
		}
		owned[pod.Name] = pod
		ready := podRunningAndReady(pod)
		terminating = terminating || pod.DeletionTimestamp != nil
		allAvailable = allAvailable && statefulSetPodAvailable(pod, set.Spec.MinReadySeconds, now)
		revision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
		if ordinal < start+partition || ordinal <= highest || revision == update ||
			pod.DeletionTimestamp != nil || (!onDelete && ready) {
			continue
		}
		highest = ordinal
		next = &StuckRestartPod{Pod: pod, requireSameVersion: true,
			Reason: fmt.Sprintf("Pod revision %q differs from restored revision %q", revision, update)}
	}
	if next == nil {
		return nil, false
	}
	if terminating {
		return nil, true
	}
	// A prior replacement must become available before making another.
	for _, effect := range effects {
		if effect.Operation != "delete" || effect.Ref.Kind != "Pod" || effect.Ref.Namespace != set.Namespace ||
			effect.State == "not_applied" {
			continue
		}
		ordinal, valid := statefulSetPodOrdinal(set.Name, effect.Ref.Name)
		if !valid || ordinal < start+partition || ordinal >= start+replicas {
			continue
		}
		pod, exists := owned[effect.Ref.Name]
		if !exists || string(pod.UID) == effect.UID || !statefulSetPodAvailable(pod, set.Spec.MinReadySeconds, now) {
			return nil, true
		}
	}
	// An OnDelete repair may take a healthy replica down only when all desired
	// replicas are available. RollingUpdate assistance never reduces readiness.
	if podRunningAndReady(next.Pod) && (!allAvailable || len(owned) < replicas) {
		return nil, true
	}
	return next, true
}

func statefulSetPodOrdinal(setName, podName string) (int, bool) {
	suffix, matches := strings.CutPrefix(podName, setName+"-")
	ordinal, err := strconv.Atoi(suffix)
	return ordinal, matches && err == nil
}

func statefulSetPodAvailable(pod corev1.Pod, minReadySeconds int32, now time.Time) bool {
	if pod.DeletionTimestamp != nil || !podRunningAndReady(pod) {
		return false
	}
	if minReadySeconds == 0 {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return !condition.LastTransitionTime.IsZero() &&
				!now.Before(condition.LastTransitionTime.Add(time.Duration(minReadySeconds)*time.Second))
		}
	}
	return false
}
