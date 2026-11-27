package install

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// waitPodReady waits for the Pod to be Ready. SyncTimeout (default 180 s) is a stall limit, not a deadline: it
// restarts whenever the Pod or a PVC it mounts advances (phase, conditions, container states, a claim binding),
// and a claim still being provisioned counts as progress until the cluster reports that provisioning failed or
// its StorageClass is missing, which fails at once. Only the caller's ctx bounds the total time.
func (i Installer) waitPodReady(ctx context.Context, namespace, name string) error {
	stall := i.SyncTimeout
	if stall <= 0 {
		stall = 180 * time.Second
	}
	var last *corev1.Pod
	signature := ""
	progressAt := i.now()
	for {
		pod, err := i.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			last = pod
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					return nil
				}
			}
			if pod.Status.Phase == corev1.PodFailed {
				return fmt.Errorf("%s Pod failed", name)
			}
			if refused := i.resourceRefusal(pod); refused != nil {
				return refused
			}
			current, provisioning, claimErr := i.podProgress(ctx, namespace, pod)
			if claimErr != nil {
				return fmt.Errorf("%s Pod cannot become ready: %w", name, claimErr)
			}
			if current != signature || provisioning {
				signature, progressAt = current, i.now()
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		remaining := stall - i.now().Sub(progressAt)
		var waitErr error
		if remaining <= 0 {
			waitErr = fmt.Errorf("no progress for %s: %w", stall, context.DeadlineExceeded)
		} else {
			pauseCtx, cancel := context.WithTimeout(ctx, remaining)
			waitErr = i.pause(pauseCtx)
			limited := pauseCtx.Err() != nil
			cancel()
			if waitErr != nil && limited && ctx.Err() == nil && i.now().Sub(progressAt) < stall {
				// The pause's own limit expired on the real clock before the installer clock reached the stall limit.
				waitErr = nil
			}
		}
		if waitErr != nil {
			if ctx.Err() != nil {
				waitErr = ctx.Err()
			}
			return fmt.Errorf("%s Pod did not become ready: %w%s", name, waitErr, i.podEvidence(ctx, namespace, name, last))
		}
	}
}

// podProgress summarizes what can advance while a Pod starts: its phase, conditions, container states and the
// claims it mounts. provisioning is true while a mounted claim is Pending with no sign provisioning failed; a
// claim whose provisioning failed or whose StorageClass is missing is an error naming the claim and the cluster's
// message.
func (i Installer) podProgress(ctx context.Context, namespace string, pod *corev1.Pod) (signature string, provisioning bool, err error) {
	parts := []string{string(pod.Status.Phase)}
	for _, condition := range pod.Status.Conditions {
		parts = append(parts, fmt.Sprintf("%s=%s", condition.Type, condition.Status))
	}
	for _, status := range pod.Status.ContainerStatuses {
		state := "none"
		switch {
		case status.State.Running != nil:
			state = "running"
		case status.State.Terminated != nil:
			state = "terminated"
		case status.State.Waiting != nil:
			state = "waiting " + status.State.Waiting.Reason
		}
		parts = append(parts, fmt.Sprintf("%s:%s:%t:%d", status.Name, state, status.Ready, status.RestartCount))
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		claimName := volume.PersistentVolumeClaim.ClaimName
		claim, getErr := i.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			parts = append(parts, "pvc "+claimName+" missing")
			continue
		}
		if getErr != nil {
			return "", false, getErr
		}
		parts = append(parts, fmt.Sprintf("pvc %s %s", claimName, claim.Status.Phase))
		if claim.Status.Phase != corev1.ClaimPending {
			continue
		}
		if class := claim.Spec.StorageClassName; class != nil && *class != "" {
			if _, classErr := i.Client.StorageV1().StorageClasses().Get(ctx, *class, metav1.GetOptions{}); apierrors.IsNotFound(classErr) {
				return "", false, fmt.Errorf("PVC %s is Pending and its StorageClass %q does not exist", claimName, *class)
			}
		}
		if events, listErr := i.Client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{}); listErr == nil {
			for _, event := range events.Items {
				if event.InvolvedObject.Kind == "PersistentVolumeClaim" && event.InvolvedObject.Name == claimName &&
					event.Type == corev1.EventTypeWarning && event.Reason == "ProvisioningFailed" {
					return "", false, fmt.Errorf("PVC %s provisioning failed: %s", claimName, event.Message)
				}
			}
		}
		provisioning = true
	}
	return strings.Join(parts, ";"), provisioning, nil
}
