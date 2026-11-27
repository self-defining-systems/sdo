package install

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/devmount"
)

// projectInstall gives the sync Pod what the evidence archive needs after the controller exited: the credential
// Secrets of the namespace's controller (or operate) Job, read-only, to redact with, and the Job's development
// operator binary. A Secret that is gone is not mounted: the archive then withholds transcripts instead of the Pod
// never starting.
func (i Installer) projectInstall(ctx context.Context, pod *corev1.Pod) error {
	jobs, err := i.Client.BatchV1().Jobs(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("find the installed Job: %w", err)
	}
	mounted, developed := map[string]bool{}, false
	// The controller Job first: it is the current install; an operate Job is a finished one.
	for _, controller := range []bool{true, false} {
		for index := range jobs.Items {
			job := &jobs.Items[index]
			if (job.Name == ControllerJobName) != controller || !controller && job.Spec.Template.Labels["app.kubernetes.io/name"] != "sdo-operate" {
				continue
			}
			spec := job.Spec.Template.Spec
			for _, volume := range spec.Volumes {
				path := credentialMounts[volume.Name]
				if path == "" || volume.Secret == nil || mounted[volume.Name] {
					continue
				}
				_, err := i.Client.CoreV1().Secrets(pod.Namespace).Get(ctx, volume.Secret.SecretName, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					continue
				}
				if err != nil {
					return fmt.Errorf("inspect credentials Secret: %w", err)
				}
				mounted[volume.Name] = true
				mountInSync(pod, volume, corev1.VolumeMount{Name: volume.Name, MountPath: path, ReadOnly: true})
			}
			if !developed {
				developed = mountDevelopedOperator(pod, spec)
			}
		}
	}
	if !developed && devmount.Enabled() {
		devmount.Apply(&pod.Spec)
	}
	return nil
}

var credentialMounts = map[string]string{"credentials": "/sdo/credentials", "claude-accounts": "/sdo/claude-accounts"}

const operatorBinary = "/usr/local/bin/sdo-operator"

func mountDevelopedOperator(pod *corev1.Pod, spec corev1.PodSpec) bool {
	if len(spec.Containers) == 0 {
		return false
	}
	for _, mount := range spec.Containers[0].VolumeMounts {
		if mount.MountPath != operatorBinary {
			continue
		}
		for _, volume := range spec.Volumes {
			if volume.Name == mount.Name {
				mount.ReadOnly = true
				mountInSync(pod, volume, mount)
				return true
			}
		}
	}
	return false
}

func mountInSync(pod *corev1.Pod, volume corev1.Volume, mount corev1.VolumeMount) {
	for _, existing := range pod.Spec.Volumes {
		// A second volume on a claim the Pod already mounts never mounts.
		if existing.Name == volume.Name {
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mount)
			return
		}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, *volume.DeepCopy())
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mount)
}
