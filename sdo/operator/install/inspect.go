package install

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InspectConfig identifies a live controller without trusting a local checkout.
type InspectConfig struct {
	ControlNamespace, Namespace, Application string
	RepositoryPVC, ControllerImage           string
}

// ExistingController is a read-only continuity receipt for a persistent run.
type ExistingController struct {
	JobUID         string `json:"job_uid"`
	PodUID         string `json:"pod_uid"`
	Fingerprint    string `json:"fingerprint"`
	AcceptedCommit string `json:"accepted_commit"`
}

// InspectExisting accepts only the current active native Job and its ready Pod.
// ReadAcceptedCommit must read the accepted HEAD from the shared PVC through a
// trusted host transport; a stage's local checkout may lag learned memory.
func (i Installer) InspectExisting(ctx context.Context, cfg InspectConfig) (ExistingController, error) {
	if i.Client == nil || i.ReadAcceptedCommit == nil {
		return ExistingController{}, errors.New("Kubernetes client and accepted HEAD reader are required")
	}
	if !namespaceName.MatchString(cfg.ControlNamespace) || !namespaceName.MatchString(cfg.Namespace) || strings.TrimSpace(cfg.Application) == "" || !namespaceName.MatchString(cfg.RepositoryPVC) || strings.TrimSpace(cfg.ControllerImage) == "" {
		return ExistingController{}, errors.New("inspect-existing requires controller identity, namespace, PVC, and image")
	}
	job, err := i.Client.BatchV1().Jobs(cfg.ControlNamespace).Get(ctx, ControllerJobName, metav1.GetOptions{})
	if err != nil {
		return ExistingController{}, fmt.Errorf("read existing controller Job: %w", err)
	}
	fingerprint := job.Annotations[InstallFingerprintAnnotation]
	if job.Status.Active != 1 || jobFailed(job) || fingerprint == "" || job.Annotations[WorkerMountPolicyAnnotation] != "proposal-only" || job.UID == "" {
		return ExistingController{}, errors.New("existing controller Job is not an active native installation")
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		return ExistingController{}, errors.New("existing controller Job has unexpected containers")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Name != "controller" || container.Image != cfg.ControllerImage || len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/sdo-operator" || !hasArg(container.Args, "--broker-mode=native") || !hasArg(container.Args, "--worker-mount-mode=proposal-only") || !argValueEquals(container.Args, "--namespace", cfg.Namespace) || !argValueEquals(container.Args, "--application", cfg.Application) {
		return ExistingController{}, errors.New("existing controller Job identity or native mode differs")
	}
	pvcMatches := false
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "repository" && volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == cfg.RepositoryPVC {
			pvcMatches = true
		}
	}
	if !pvcMatches {
		return ExistingController{}, errors.New("existing controller Job uses a different repository PVC")
	}
	pods, err := i.Client.CoreV1().Pods(cfg.ControlNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return ExistingController{}, fmt.Errorf("list controller Pods: %w", err)
	}
	var podUID string
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning || !podReady(pod.Status.Conditions) {
			continue
		}
		owned := false
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && owner.Name == ControllerJobName && owner.UID == job.UID {
				owned = true
			}
		}
		if owned {
			if podUID != "" || pod.UID == "" {
				return ExistingController{}, errors.New("existing controller requires exactly one ready owned Pod")
			}
			podUID = string(pod.UID)
		}
	}
	if podUID == "" {
		return ExistingController{}, errors.New("existing controller has no ready owned Pod")
	}
	commit, err := i.ReadAcceptedCommit(ctx, SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage})
	if err != nil {
		return ExistingController{}, fmt.Errorf("read accepted PVC HEAD: %w", err)
	}
	if !commitID.MatchString(commit) {
		return ExistingController{}, errors.New("accepted PVC HEAD is not a full Git commit")
	}
	return ExistingController{JobUID: string(job.UID), PodUID: podUID, Fingerprint: fingerprint, AcceptedCommit: commit}, nil
}

func argValueEquals(args []string, key, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == key && args[index+1] == value {
			return true
		}
	}
	return false
}

func podReady(conditions []corev1.PodCondition) bool {
	for _, condition := range conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
