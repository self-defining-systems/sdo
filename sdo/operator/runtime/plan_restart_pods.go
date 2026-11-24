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
	"sdo.dev/operator/runtime/internal/lifecycleio"
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

// sourceRestoredStatefulSets returns the StatefulSet source writes, including
// the identity and fields the executor just verified.
func sourceRestoredStatefulSets(plan program.Plan, result PlanApplicationResult) []PlanPatch {
	var restores []PlanPatch
	for _, patch := range result.Patches {
		if patch.Ref.Kind != "StatefulSet" || patch.Before == nil || patch.After == nil {
			continue
		}
		for _, action := range plan.Actions {
			if ref, err := planActionRef(action); err == nil && ref == patch.Ref && isSourceReconcile(action) {
				restores = append(restores, patch)
				break
			}
		}
	}
	return restores
}

// replaceRestoredStatefulSetPods completes the source restore within the
// application's mutation fence. Kubernetes recreates eligible Pods at the
// observed update revision.

func (e PlanExecutor) replaceRestoredStatefulSetPods(ctx context.Context, restores []PlanPatch, result *PlanApplicationResult) error {
	lister, ok := e.Client.(planPodLister)
	if !ok || len(restores) == 0 {
		return nil
	}
	for _, restore := range restores {
		ref := restore.Ref
		set, err := e.awaitObservedStatefulSet(ctx, restore)
		if err != nil {
			return fmt.Errorf("read StatefulSet %s after its source restore: %w", ref.Name, err)
		}
		pods, err := lister.NamespacePods(ctx, ref.Namespace)
		if err != nil {
			return fmt.Errorf("list Pods of StatefulSet %s: %w", ref.Name, err)
		}
		stuck, err := podsOffUpdateRevision(set, pods)
		if err != nil {
			return err
		}
		if err := e.replaceStuckPods(ctx, stuck, result); err != nil {
			return err
		}
	}
	return nil
}

const templateRolloutWaitBound = 10 * time.Second

// awaitObservedStatefulSet reads the StatefulSet once its controller has
// observed the restored template, so status.updateRevision names that template.
func (e PlanExecutor) awaitObservedStatefulSet(ctx context.Context, restore PlanPatch) (*unstructured.Unstructured, error) {
	deadline := lifecycleio.Now().Add(templateRolloutWaitBound)
	template, _, _ := unstructured.NestedMap(restore.After.Object, "spec", "template")
	for {
		set, err := e.Client.Get(ctx, restore.Ref)
		if err != nil {
			return nil, err
		}
		liveTemplate, _, _ := unstructured.NestedMap(set.Object, "spec", "template")
		if set.GetUID() != restore.Before.GetUID() || !reflect.DeepEqual(liveTemplate, template) {
			return nil, fmt.Errorf("StatefulSet identity or template changed after the source restore")
		}
		observed, _, _ := unstructured.NestedInt64(set.Object, "status", "observedGeneration")
		if revision, _, _ := unstructured.NestedString(set.Object, "status", "updateRevision"); observed >= set.GetGeneration() && revision != "" {
			return set, nil
		}
		if !lifecycleio.Now().Before(deadline) {
			return nil, fmt.Errorf("controller did not publish the restored revision")
		}
		if err := lifecycleio.Wait(ctx, 250*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

// podsOffUpdateRevision returns the owned Pods eligible for replacement in
// ordinal order, preserving the rolling-update partition.
func podsOffUpdateRevision(workload *unstructured.Unstructured, pods []corev1.Pod) ([]StuckRestartPod, error) {
	var set appsv1.StatefulSet
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(workload.Object, &set); err != nil {
		return nil, err
	}
	update := set.Status.UpdateRevision
	if update == "" {
		return nil, nil
	}
	partition := 0
	if rolling := set.Spec.UpdateStrategy.RollingUpdate; rolling != nil && rolling.Partition != nil {
		partition = int(*rolling.Partition)
	}
	type ordinalPod struct {
		ordinal int
		pod     corev1.Pod
		reason  string
	}
	var found []ordinalPod
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if pod.DeletionTimestamp != nil || owner == nil || owner.Kind != "StatefulSet" || owner.Name != set.Name || pod.Namespace != set.Namespace || (set.UID != "" && owner.UID != set.UID) {
			continue
		}
		revision := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
		ordinal, err := strconv.Atoi(strings.TrimPrefix(pod.Name, set.Name+"-"))
		if revision == update || err != nil || ordinal < partition {
			continue
		}
		found = append(found, ordinalPod{ordinal, pod, fmt.Sprintf("Pod revision %q differs from restored revision %q", revision, update)})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].ordinal < found[j].ordinal })
	var stuck []StuckRestartPod
	for _, item := range found {
		stuck = append(stuck, StuckRestartPod{Pod: item.pod, Reason: item.reason})
	}
	return stuck, nil
}
