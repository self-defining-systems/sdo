package core

import (
	"context"
	"errors"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/core/internal/kubeio"
	"sdo.dev/operator/sdk"
)

// ObservePodAdmission issues exactly one bounded, server-side dry-run create
// in the application namespace and returns its result and the Pod it
// submitted. The Pod is built from the pod template of an existing app Pod's
// controller (ReplicaSet, StatefulSet or DaemonSet) when the view holds it, so
// admission mutates it as it would the workload's next Pod; otherwise the app
// Pod's admission inputs are copied, dropping allocated identity, scheduling
// state and ephemeral containers. The fallback also probes an empty
// namespace; the image is never pulled. The result records how long the API
// server took and which fields admission changed.
func ObservePodAdmission(ctx context.Context, client kubernetes.Interface, view sdk.DetectionContext) (sdk.PodAdmissionResult, *corev1.Pod) {
	namespace := view.Namespace()
	r := sdk.PodAdmissionResult{Namespace: namespace}
	if client == nil || namespace == "" {
		r.Error = "Kubernetes client and application namespace are required"
		return r, nil
	}
	candidates := make([]corev1.Pod, 0)
	for _, pod := range view.Pods() {
		if pod.Namespace == namespace && pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			candidates = append(candidates, pod)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	pod := &corev1.Pod{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "admission", Image: "registry.k8s.io/pause:3.9"}}}}
	if len(candidates) > 0 {
		r.SourcePod = candidates[0].Name
		if template := controllerTemplate(view, candidates[0]); template != nil {
			pod = &corev1.Pod{ObjectMeta: template.ObjectMeta, Spec: template.Spec}
		} else {
			pod = candidates[0].DeepCopy()
		}
	}
	pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	pod.ObjectMeta = metav1.ObjectMeta{Namespace: namespace, GenerateName: "sdo-admission-", Labels: pod.Labels, Annotations: pod.Annotations}
	pod.Status = corev1.PodStatus{}
	pod.Spec.NodeName = ""
	pod.Spec.EphemeralContainers = nil
	pod.Spec.Overhead = nil
	submitted := pod.DeepCopy()
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := kubeio.Now()
	admitted, err := client.CoreV1().Pods(namespace).Create(probeCtx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	r.Latency = kubeio.Now().Sub(started)
	if err == nil {
		r.Admitted = true
		if admitted != nil {
			r.Mutations = sdk.AdmissionMutations(*submitted, *admitted)
		}
		return r, submitted
	}
	var status apierrors.APIStatus
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || !errors.As(err, &status) {
		r.Error = err.Error()
	} else {
		r.Rejection = err.Error()
	}
	return r, submitted
}

// controllerTemplate returns the pod template of the Pod's controlling
// ReplicaSet, StatefulSet or DaemonSet in the view, nil when absent.
func controllerTemplate(view sdk.DetectionContext, pod corev1.Pod) *corev1.PodTemplateSpec {
	owner := metav1.GetControllerOf(&pod)
	if owner == nil {
		return nil
	}
	switch owner.Kind {
	case "ReplicaSet":
		for _, item := range view.ReplicaSets() {
			if item.Name == owner.Name && item.Namespace == pod.Namespace {
				return item.Spec.Template.DeepCopy()
			}
		}
	case "StatefulSet":
		for _, item := range view.StatefulSets() {
			if item.Name == owner.Name && item.Namespace == pod.Namespace {
				return withClaimVolumes(item.Spec.Template.DeepCopy(), item, pod)
			}
		}
	case "DaemonSet":
		for _, item := range view.DaemonSets() {
			if item.Name == owner.Name && item.Namespace == pod.Namespace {
				return item.Spec.Template.DeepCopy()
			}
		}
	}
	return nil
}

// withClaimVolumes adds the volumes the StatefulSet controller adds to each Pod
// for its volumeClaimTemplates: the template alone mounts volumes it does not
// declare, which admission rejects. The source Pod's own volume is reused when
// it has one; otherwise the ordinal-0 claim name the controller would use.
func withClaimVolumes(template *corev1.PodTemplateSpec, set appsv1.StatefulSet, source corev1.Pod) *corev1.PodTemplateSpec {
	declared := map[string]bool{}
	for _, volume := range template.Spec.Volumes {
		declared[volume.Name] = true
	}
	for _, claim := range set.Spec.VolumeClaimTemplates {
		if declared[claim.Name] {
			continue
		}
		volume := corev1.Volume{Name: claim.Name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name + "-" + set.Name + "-0"}}}
		for _, own := range source.Spec.Volumes {
			if own.Name == claim.Name {
				volume = *own.DeepCopy()
			}
		}
		template.Spec.Volumes = append(template.Spec.Volumes, volume)
		declared[claim.Name] = true
	}
	return template
}
