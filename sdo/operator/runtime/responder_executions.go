package runtime

import (
	"context"
	"fmt"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory"
)

type responderExecutionInventory struct {
	names          map[string]bool
	jobs           []batchv1.Job
	pods           []corev1.Pod
	repositoryPVC  string
	sessionSubPath string
}

// Both retirement and report-grace shutdown consume this inventory. The registry
// owns current per-attempt identities; ECF's journal and exact session ownership
// retain recovery for workers published before that index was introduced.
func (d KubernetesJobDispatcher) responderExecutions(ctx context.Context, id string, attempts int) (*responderExecutionInventory, error) {
	names, err := d.responderJobNames(ctx, id, attempts)
	if err != nil {
		return nil, err
	}
	inventory := &responderExecutionInventory{names: names, repositoryPVC: d.RepositoryPVC}
	if d.RepositoryPVC != "" {
		inventory.sessionSubPath, err = memory.IncidentSessionSubPath(d.RepositoryPVCSubPath, id)
		if err != nil {
			return nil, fmt.Errorf("resolve responder session ownership: %w", err)
		}
	}
	// A failed exact lookup is uncertainty, even when the broad inventory API
	// remains available. Keep the existing named-worker recovery contract.
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		if _, err := d.Client.BatchV1().Jobs(d.Namespace).Get(ctx, name, metav1.GetOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("inspect incident worker %s: %w", name, err)
		}
	}
	jobs, err := d.Client.BatchV1().Jobs(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("enumerate responder Jobs: %w", err)
	}
	pods, err := d.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("enumerate responder Pods: %w", err)
	}
	inventory.jobs, inventory.pods = jobs.Items, pods.Items
	for _, job := range inventory.jobs {
		if inventory.ownsSession(job.Spec.Template.Spec) {
			names[job.Name] = true
		}
	}
	return inventory, nil
}
func (i *responderExecutionInventory) ownsSession(spec corev1.PodSpec) bool {
	if i.sessionSubPath == "" {
		return false
	}
	volumes := map[string]bool{}
	for _, volume := range spec.Volumes {
		volumes[volume.Name] = volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == i.repositoryPVC
	}
	containers := append(append([]corev1.Container(nil), spec.Containers...), spec.InitContainers...)
	for _, container := range containers {
		for _, mount := range container.VolumeMounts {
			if volumes[mount.Name] && mount.MountPath == "/sdo/session" && mount.SubPath == i.sessionSubPath && mount.SubPathExpr == "" {
				return true
			}
		}
	}
	return false
}
func (i *responderExecutionInventory) ownsPod(pod corev1.Pod) bool {
	if i.ownsSession(pod.Spec) {
		return true
	}
	for _, key := range []string{"sdo.dev/incident", "job-name", "batch.kubernetes.io/job-name"} {
		if i.names[pod.Labels[key]] {
			return true
		}
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && i.names[owner.Name] {
			return true
		}
	}
	return false
}
func terminalResponderPod(pod corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}
// finished reports a terminal responder Job, whose result a re-dispatch adopts.
func (i *responderExecutionInventory) finished() bool {
	for _, job := range i.jobs {
		if i.names[job.Name] && job.DeletionTimestamp == nil && terminalResponderJob(job) {
			return true
		}
	}
	return false
}
func terminalResponderJob(job batchv1.Job) bool {
	terminal := job.Status.CompletionTime != nil
	for _, condition := range job.Status.Conditions {
		terminal = terminal || condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed)
	}
	return terminal
}
func (i *responderExecutionInventory) active() bool {
	for _, job := range i.jobs {
		if !i.names[job.Name] {
			continue
		}
		if job.Status.Active > 0 || job.DeletionTimestamp != nil || !terminalResponderJob(job) {
			return true
		}
	}
	for _, pod := range i.pods {
		if i.ownsPod(pod) && !terminalResponderPod(pod) {
			return true
		}
	}
	return false
}
