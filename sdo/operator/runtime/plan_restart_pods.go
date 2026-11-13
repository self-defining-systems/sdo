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

// podNotReadyBound is how long a Pod may stay not Ready on an old revision
// before a source restore replaces it, when no container reports a failing wait.
const podNotReadyBound = 30 * time.Second

// templateRolloutWaitBound bounds the wait for a StatefulSet controller to
// observe a restored template and publish its updateRevision.
const templateRolloutWaitBound = 10 * time.Second

// stuckWaitingReasons are container waiting reasons that never resolve alone.
var stuckWaitingReasons = map[string]bool{"ImagePullBackOff": true, "ErrImagePull": true, "CrashLoopBackOff": true, "CreateContainerConfigError": true, "InvalidImageName": true}

// sourceRestoredStatefulSets returns the StatefulSets whose pod template a
// ReconcileFromSource in this plan actually changed.
func sourceRestoredStatefulSets(plan program.Plan, result PlanApplicationResult) []sdk.ObjectRef {
	var refs []sdk.ObjectRef
	for _, patch := range result.Patches {
		if patch.Ref.Kind != "StatefulSet" || patch.Before == nil || patch.After == nil {
			continue
		}
		restored := false
		for _, action := range plan.Actions {
			if ref, err := planActionRef(action); err == nil && ref == patch.Ref && isSourceReconcile(action) {
				restored = true
			}
		}
		before, _, _ := unstructured.NestedMap(patch.Before.Object, "spec", "template")
		after, _, _ := unstructured.NestedMap(patch.After.Object, "spec", "template")
		if restored && !reflect.DeepEqual(before, after) {
			refs = append(refs, patch.Ref)
		}
	}
	return refs
}

// replaceRevisionStuckPods completes a source restore of a StatefulSet template
// (Kubernetes "forced rollback"): the controller will not replace a Pod that is
// stuck not Ready on the bad revision, so each such Pod is deleted, in ordinal
// order, and recreated at the restored updateRevision. Only Pods on another
// revision that are failing (ImagePullBackOff, ErrImagePull, CrashLoopBackOff...)
// or not Ready past podNotReadyBound are deleted, never a Ready Pod.
func (e PlanExecutor) replaceRevisionStuckPods(ctx context.Context, refs []sdk.ObjectRef, result *PlanApplicationResult) error {
	lister, ok := e.Client.(planPodLister)
	if !ok || len(refs) == 0 {
		return nil
	}
	now := lifecycleio.Now
	if e.Now != nil {
		now = e.Now
	}
	for _, ref := range refs {
		set, err := e.awaitObservedStatefulSet(ctx, ref)
		if err != nil {
			return fmt.Errorf("read StatefulSet %s after its source restore: %w", ref.Name, err)
		}
		pods, err := lister.NamespacePods(ctx, ref.Namespace)
		if err != nil {
			return fmt.Errorf("list Pods of StatefulSet %s: %w", ref.Name, err)
		}
		stuck, err := podsStuckOnOldRevision(set, pods, now())
		if err != nil {
			return err
		}
		if err := e.replaceStuckPods(ctx, stuck, result); err != nil {
			return err
		}
	}
	return nil
}

// awaitObservedStatefulSet reads the StatefulSet once its controller has
// observed the restored template, so status.updateRevision names that template.
func (e PlanExecutor) awaitObservedStatefulSet(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	deadline := lifecycleio.Now().Add(templateRolloutWaitBound)
	for {
		set, err := e.Client.Get(ctx, ref)
		if err != nil {
			return nil, err
		}
		observed, _, _ := unstructured.NestedInt64(set.Object, "status", "observedGeneration")
		if observed >= set.GetGeneration() || !lifecycleio.Now().Before(deadline) {
			return set, nil
		}
		if lifecycleio.Wait(ctx, 250*time.Millisecond) != nil {
			return set, nil
		}
	}
}

// podsStuckOnOldRevision returns the StatefulSet's Pods the controller will not
// replace after a template restore, in ordinal order: owned, not terminating, on
// a revision other than status.updateRevision, and failing or not Ready past
// podNotReadyBound. A Pod without a revision label, or a set without an
// updateRevision, cannot be compared and is left alone.
func podsStuckOnOldRevision(workload *unstructured.Unstructured, pods []corev1.Pod, now time.Time) ([]StuckRestartPod, error) {
	var set appsv1.StatefulSet
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(workload.Object, &set); err != nil {
		return nil, err
	}
	update := set.Status.UpdateRevision
	if update == "" || set.Spec.PodManagementPolicy == appsv1.ParallelPodManagement {
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
		if revision == "" || revision == update || err != nil || ordinal < partition || podRunningAndReady(pod) {
			continue
		}
		reason := podStuckReason(pod, now)
		if reason == "" {
			continue
		}
		found = append(found, ordinalPod{ordinal, pod, fmt.Sprintf("Pod is on revision %s, not the restored revision %s, and %s: a StatefulSet rolling update never replaces a Pod that is not Ready, so it is deleted to be recreated at the restored revision", revision, update, reason)})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].ordinal < found[j].ordinal })
	var stuck []StuckRestartPod
	for _, item := range found {
		stuck = append(stuck, StuckRestartPod{Pod: item.pod, Reason: item.reason})
	}
	return stuck, nil
}

// podStuckReason says why a not-Ready Pod will not recover on its own, or "".
func podStuckReason(pod corev1.Pod, now time.Time) string {
	for _, status := range append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
		if waiting := status.State.Waiting; waiting != nil && stuckWaitingReasons[waiting.Reason] {
			return "a container is waiting with " + waiting.Reason
		}
	}
	since := pod.CreationTimestamp.Time
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && !condition.LastTransitionTime.IsZero() {
			since = condition.LastTransitionTime.Time
		}
	}
	if !since.IsZero() && now.Sub(since) >= podNotReadyBound {
		return "has not been Ready for " + now.Sub(since).Round(time.Second).String()
	}
	return ""
}
