package core

import (
	"context"
	"errors"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/sdk"
)

// ObservePodAdmission issues exactly one bounded, server-side dry-run create
// in the application namespace. It copies an existing app Pod's admission
// inputs, dropping allocated identity, scheduling state and ephemeral containers.
// The fallback also probes an empty namespace; the image is never pulled.
func ObservePodAdmission(ctx context.Context, client kubernetes.Interface, namespace string, pods []corev1.Pod) sdk.PodAdmissionResult {
	r := sdk.PodAdmissionResult{Namespace: namespace}
	if client == nil || namespace == "" {
		r.Error = "Kubernetes client and application namespace are required"
		return r
	}
	candidates := make([]corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.Namespace == namespace && pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			candidates = append(candidates, pod)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	pod := &corev1.Pod{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "admission", Image: "registry.k8s.io/pause:3.9"}}}}
	if len(candidates) > 0 {
		pod = candidates[0].DeepCopy()
		r.SourcePod = pod.Name
	}
	pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	pod.ObjectMeta = metav1.ObjectMeta{Namespace: namespace, GenerateName: "sdo-admission-", Labels: pod.Labels, Annotations: pod.Annotations}
	pod.Status = corev1.PodStatus{}
	pod.Spec.NodeName = ""
	pod.Spec.EphemeralContainers = nil
	pod.Spec.Overhead = nil
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := client.CoreV1().Pods(namespace).Create(probeCtx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		r.Admitted = true
		return r
	}
	var status apierrors.APIStatus
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || !errors.As(err, &status) {
		r.Error = err.Error()
	} else {
		r.Rejection = err.Error()
	}
	return r
}
