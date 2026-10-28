package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// settleSourceDependencies shares the existing rollout budget. Recovery is a
// single pass over the pre-recovery Pod identities, never an ongoing restart loop.
func (e PlanExecutor) settleSourceDependencies(ctx context.Context, incident IncidentRequest, result *PlanApplicationResult) error {
	if e.deferConvergence {
		return nil
	}
	recovery, ok := e.Rollouts.(interface {
		RecoverDependencies(context.Context, []sdk.ObjectRef) ([]PlanObjectEffect, error)
	})
	if !ok {
		return nil
	}
	restored := []sdk.ObjectRef{}
	for _, patch := range result.Patches {
		if patch.Operation == "delete" || patch.Operation == "delete_pod" {
			continue
		}
		if patch.Authorization == "accepted-source" || patch.Operation == "ensure_database_user" || (patch.Ref.Kind == "ConfigMap" && patch.After != nil) {
			restored = append(restored, patch.Ref)
		}
	}
	if len(restored) == 0 {
		return nil
	}
	effects, err := recovery.RecoverDependencies(ctx, restored)
	result.ObjectEffects = append(result.ObjectEffects, effects...)
	return err
}

func (w KubernetesPlanRollouts) RecoverDependencies(ctx context.Context, restored []sdk.ObjectRef) ([]PlanObjectEffect, error) {
	var effects []PlanObjectEffect
	if w.Client == nil {
		return nil, fmt.Errorf("Kubernetes dependency client is required")
	}
	namespaces := map[string][]sdk.ObjectRef{}
	for _, ref := range restored {
		if ref.Namespace != "" {
			namespaces[ref.Namespace] = append(namespaces[ref.Namespace], ref)
		}
	}
	for namespace, refs := range namespaces {
		pods, err := w.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return effects, fmt.Errorf("list restored dependency consumers: %w", err)
		}
		workloadSelectors := map[sdk.ObjectRef]labels.Selector{}
		for _, ref := range refs {
			if ref.Kind == "Deployment" && ref.APIVersion == "apps/v1" {
				object, err := w.Client.AppsV1().Deployments(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					return effects, err
				}
				selector, err := metav1.LabelSelectorAsSelector(object.Spec.Selector)
				if err != nil {
					return effects, err
				}
				workloadSelectors[ref] = selector
			}
		}
		backends := map[string]bool{}
		recycled := map[string]bool{}
		recycle := func(pod *corev1.Pod, consumer bool) error {
			if recycled[string(pod.UID)] {
				return nil
			}
			// Recheck the same incarnation immediately before deletion: a
			// consumer may have recovered while its backend became Ready.
			current, getErr := w.Client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return nil
			}
			if getErr != nil {
				return getErr
			}
			if current.UID != pod.UID || (!consumer && !restartableBlockedPod(current)) || (consumer && !restartableConsumerPod(current)) {
				return nil
			}
			pod = current
			uid, rv := pod.UID, pod.ResourceVersion
			if uid == "" || rv == "" {
				return fmt.Errorf("dependent Pod identity is unavailable")
			}
			effects = append(effects, PlanObjectEffect{Operation: "delete", Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: pod.Name}, UID: string(uid), State: "unknown", Irreversible: true})
			effect := &effects[len(effects)-1]
			recycled[string(uid)] = true
			err := w.Client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
			if err != nil {
				if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsConflict(err) {
					effect.State = "not_applied"
				}
				return fmt.Errorf("recycle dependent Pod %s: %w", pod.Name, err)
			}
			live, getErr := w.Client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			effect.Confirmed = apierrors.IsNotFound(getErr) || (getErr == nil && (live.UID != uid || live.DeletionTimestamp != nil))
			if effect.Confirmed {
				effect.State = "applied"
			}
			if !effect.Confirmed {
				return fmt.Errorf("dependent Pod deletion was not observed")
			}
			return nil
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			for _, ref := range refs {
				selector, workload := workloadSelectors[ref]
				if podUsesRestoredObject(pod, ref) || (workload && selector.Matches(labels.Set(pod.Labels))) {
					backends[pod.Name] = true
					if (ref.Kind == "ConfigMap" || ref.Kind == "Secret") && restartableBlockedPod(pod) {
						if err := recycle(pod, false); err != nil {
							return effects, err
						}
					}
				}
			}
		}
		if len(backends) == 0 {
			continue
		}
		services, err := w.Client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return effects, fmt.Errorf("list dependency relations: %w", err)
		}
		for _, service := range services.Items {
			if len(service.Spec.Selector) == 0 {
				continue
			}
			selector := labels.SelectorFromSet(service.Spec.Selector)
			related := false
			for _, pod := range pods.Items {
				if backends[pod.Name] && selector.Matches(labels.Set(pod.Labels)) {
					related = true
					break
				}
			}
			if !related {
				continue
			}
			consumers := []*corev1.Pod{}
			for i := range pods.Items {
				pod := &pods.Items[i]
				if !backends[pod.Name] && restartableConsumerPod(pod) && podUsesService(pod, service.Name, namespace) {
					consumers = append(consumers, pod)
				}
			}
			if len(consumers) == 0 {
				continue
			}
			// A Ready old replica is insufficient after restoring a workload:
			// confirm its accepted generation before restarting consumers.
			for ref, workloadSelector := range workloadSelectors {
				overlaps := false
				for _, pod := range pods.Items {
					if selector.Matches(labels.Set(pod.Labels)) && workloadSelector.Matches(labels.Set(pod.Labels)) {
						overlaps = true
						break
					}
				}
				if overlaps {
					if err := w.Wait(ctx, []sdk.ObjectRef{ref}); err != nil {
						return effects, err
					}
				}
			}
			// Only restart stale connections once a live backend is Ready. API status
			// is the production evidence; configuration values never establish readiness.
			for {
				current, err := w.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
				if err != nil {
					return effects, err
				}
				ready := false
				for _, pod := range current.Items {
					if pod.DeletionTimestamp == nil && podReady(&pod) {
						ready = true
						break
					}
				}
				if ready {
					break
				}
				select {
				case <-ctx.Done():
					return effects, ctx.Err()
				case <-time.After(time.Second):
				}
			}
			for _, pod := range consumers {
				if err := recycle(pod, true); err != nil {
					return effects, err
				}
			}
		}
	}
	return effects, nil
}

func restartableBlockedPod(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || podReady(pod) {
		return false
	}
	if !managedRecoveryPod(pod) {
		return false
	}
	if pod.Status.Reason == "FailedMount" {
		return true
	}
	statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.State.Waiting != nil {
			switch status.State.Waiting.Reason {
			case "ContainerCreating", "CrashLoopBackOff", "CreateContainerConfigError":
				return true
			}
		}
	}
	return false
}
func podUsesRestoredObject(pod *corev1.Pod, ref sdk.ObjectRef) bool {
	if ref.Namespace != pod.Namespace {
		return false
	}
	if ref.Kind == "Pod" {
		return ref.Name == pod.Name
	}
	for _, volume := range pod.Spec.Volumes {
		if ref.Kind == "ConfigMap" && volume.ConfigMap != nil && volume.ConfigMap.Name == ref.Name {
			return true
		}
		if ref.Kind == "Secret" && volume.Secret != nil && volume.Secret.SecretName == ref.Name {
			return true
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if ref.Kind == "ConfigMap" && source.ConfigMap != nil && source.ConfigMap.Name == ref.Name {
					return true
				}
				if ref.Kind == "Secret" && source.Secret != nil && source.Secret.Name == ref.Name {
					return true
				}
			}
		}
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		for _, env := range container.Env {
			if env.ValueFrom != nil {
				if ref.Kind == "ConfigMap" && env.ValueFrom.ConfigMapKeyRef != nil && env.ValueFrom.ConfigMapKeyRef.Name == ref.Name {
					return true
				}
				if ref.Kind == "Secret" && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == ref.Name {
					return true
				}
			}
		}
		for _, env := range container.EnvFrom {
			if ref.Kind == "ConfigMap" && env.ConfigMapRef != nil && env.ConfigMapRef.Name == ref.Name {
				return true
			}
			if ref.Kind == "Secret" && env.SecretRef != nil && env.SecretRef.Name == ref.Name {
				return true
			}
		}
	}
	return false
}
func podUsesService(pod *corev1.Pod, name, namespace string) bool {
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			value := env.Value
			endpointNamed := false
			envKey := strings.ToUpper(env.Name)
			for _, suffix := range []string{"HOST", "ADDR", "ADDRESS", "ENDPOINT", "URL", "URI", "SERVICE"} {
				if envKey == suffix || strings.HasSuffix(envKey, "_"+suffix) {
					endpointNamed = true
					break
				}
			}
			// Equal arbitrary strings are not a dependency relation. Require
			// address syntax or an explicitly endpoint-named environment key.
			if !endpointNamed && !strings.Contains(value, ":") && !strings.Contains(value, ".svc") {
				continue
			}
			if strings.Contains(value, "://") {
				parsed, err := url.Parse(value)
				if err != nil {
					continue
				}
				value = parsed.Hostname()
			} else {
				value = strings.Split(value, ":")[0]
			}
			value = strings.TrimSuffix(value, ".")
			if value == name || value == name+"."+namespace || value == name+"."+namespace+".svc" || value == name+"."+namespace+".svc.cluster.local" {
				return true
			}
		}
	}
	return false
}

func (e PlanExecutor) waitAfterSourceWrites(ctx context.Context, incident IncidentRequest, result *PlanApplicationResult, refs []sdk.ObjectRef) (*contracts.RolloutPending, error) {
	budget := e.RolloutTimeout
	if budget <= 0 || budget > DefaultMitigationRolloutTimeout {
		budget = DefaultMitigationRolloutTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if !incident.ResponseDeadline.IsZero() {
		var stop context.CancelFunc
		ctx, stop = context.WithDeadline(ctx, incident.ResponseDeadline)
		defer stop()
	}
	if err := e.settleSourceDependencies(ctx, incident, result); err != nil {
		var admission *AdmissionBlockedError
		if errors.As(err, &admission) {
			return admission.Pending, admission
		}
		if errors.Is(err, context.DeadlineExceeded) {
			pending := &contracts.RolloutPending{Reason: "dependent recovery is pending within the existing rollout budget"}
			return pending, &RolloutPendingError{Pending: pending}
		}
		return nil, err
	}
	// Keep nonterminal evidence before a wait can exhaust the API read budget.
	// Observing a residual here never refuses an accepted write.
	_ = e.observeSourceResiduals(ctx, incident, result)
	if len(refs) > 0 {
		pending, err := e.waitForRollouts(ctx, incident, refs)
		if err != nil {
			if !e.PollRollouts && ctx.Err() != nil {
				if residualErr := e.reportSourceResiduals(ctx, incident, result); residualErr != nil {
					return nil, errors.Join(err, residualErr)
				}
			}
			if pending != nil {
				_ = e.observeSourceResiduals(ctx, incident, result)
			}
			return pending, err
		}
	}
	return e.waitSourceConvergence(ctx, incident, result)
}

func managedRecoveryPod(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && (owner.Kind == "ReplicaSet" || owner.Kind == "StatefulSet" || owner.Kind == "DaemonSet") {
			return true
		}
	}
	return false
}
func restartableConsumerPod(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || podReady(pod) || !managedRecoveryPod(pod) {
		return false
	}
	return pod.Status.Phase == corev1.PodRunning || restartableBlockedPod(pod)
}
