package install

import (
	"context"
	"errors"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"time"
)

const operateClaimCreationWindow = 30 * time.Second

func ownerDeleteOptions(meta metav1.ObjectMeta) metav1.DeleteOptions {
	return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &meta.UID, ResourceVersion: &meta.ResourceVersion}}
}

func claimOwner(claim *corev1.ConfigMap) (string, string) {
	if kind := claim.Data["owner_kind"]; kind != "" {
		return kind, claim.Data["owner_name"]
	}
	owner := claim.Data["operation_id"]
	if strings.HasPrefix(owner, "sync-") || strings.HasPrefix(owner, "prepare-") {
		return "Pod", RepositorySyncPodName
	}
	return "Job", OperateJobName(owner)
}

func (i Installer) claimOwnerLive(ctx context.Context, namespace string, claim *corev1.ConfigMap) (bool, error) {
	kind, name := claimOwner(claim)
	var uid types.UID
	var live, inactive bool
	var err error
	switch kind {
	case "Job":
		var job *batchv1.Job
		job, err = i.Client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			uid = job.UID
			live = job.Status.Succeeded == 0 && !jobFailed(job) && (job.Status.Failed == 0 || job.Status.Active > 0)
			inactive = job.Spec.Suspend != nil && *job.Spec.Suspend
		}
	case "Pod":
		var pod *corev1.Pod
		pod, err = i.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			uid = pod.UID
			live = pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
			inactive = len(pod.Spec.SchedulingGates) > 0
		}
	default:
		return false, fmt.Errorf("unsupported PVC claim owner kind %q", kind)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if recorded := claim.Data["owner_uid"]; recorded != "" {
		return err == nil && string(uid) == recorded && live, nil
	}
	if err == nil && live && !inactive {
		return true, nil
	}
	return claim.CreationTimestamp.IsZero() || time.Since(claim.CreationTimestamp.Time) < operateClaimCreationWindow, nil
}

// The ConfigMap create arbitrates host launchers. Deletion uses both UID and
// resource version so an owner binding racing recovery cannot be discarded.
func (i Installer) acquireOperateLock(ctx context.Context, cfg Config, operationID string, owner metav1.OwnerReference) (*corev1.ConfigMap, error) {
	maps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	for attempt := 0; attempt < 3; attempt++ {
		claim, err := maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.controlNamespace()}, Data: map[string]string{"operation_id": operationID, "owner_kind": owner.Kind, "owner_name": owner.Name}}, metav1.CreateOptions{})
		if err == nil {
			return claim, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("claim native operate PVC: %w", err)
		}
		prior, err := maps.Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect native operate PVC claim: %w", err)
		}
		if prior.Data["operation_id"] == "" {
			return nil, errors.New("native operate PVC claim has no operation ID")
		}
		live, err := i.claimOwnerLive(ctx, cfg.controlNamespace(), prior)
		if err != nil {
			return nil, fmt.Errorf("inspect native operate PVC owner: %w", err)
		}
		if live {
			return nil, fmt.Errorf("native operate PVC is already used by operation %q", prior.Data["operation_id"])
		}
		if err := maps.Delete(ctx, OperateLockConfigMap, ownerDeleteOptions(prior.ObjectMeta)); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("release orphan native operate PVC claim: %w", err)
		}
	}
	return nil, errors.New("native operate PVC claim changed while starting Job; retry")
}

func (i Installer) bindOperateLock(ctx context.Context, cfg Config, reservation *corev1.ConfigMap, kind string, owner metav1.ObjectMeta) error {
	maps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	// Updating the acquired UID/resourceVersion makes binding an atomic fence
	// against both replacement and an orphan recovery racing this launcher.
	claim := reservation.DeepCopy()
	version := "v1"
	if kind == "Job" {
		version = "batch/v1"
	}
	claim.Data["owner_kind"], claim.Data["owner_name"], claim.Data["owner_uid"] = kind, owner.Name, string(owner.UID)
	claim.OwnerReferences = []metav1.OwnerReference{{APIVersion: version, Kind: kind, Name: owner.Name, UID: owner.UID}}
	if _, err := maps.Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("bind PVC claim to %s: %w", kind, err)
	}
	return nil
}

// The Pod cannot schedule until its reservation is bound to its exact UID.
func (i Installer) startClaimedSyncPod(ctx context.Context, cfg Config, reservation *corev1.ConfigMap, pod *corev1.Pod) error {
	pod = pod.DeepCopy()
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "sdo.dev/pvc-claim"}}
	created, err := i.Client.CoreV1().Pods(cfg.controlNamespace()).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if err := i.bindOperateLock(ctx, cfg, reservation, "Pod", created.ObjectMeta); err != nil {
		_ = i.Client.CoreV1().Pods(cfg.controlNamespace()).Delete(context.Background(), created.Name, ownerDeleteOptions(created.ObjectMeta))
		return err
	}
	created.Spec.SchedulingGates = nil
	_, err = i.Client.CoreV1().Pods(cfg.controlNamespace()).Update(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		_ = i.Client.CoreV1().Pods(cfg.controlNamespace()).Delete(context.Background(), created.Name, ownerDeleteOptions(created.ObjectMeta))
	}
	return err
}

// Cleanup must never release a later reservation, even for the same operation ID.
func (i Installer) releaseOperateReservation(ctx context.Context, cfg Config, reservation *corev1.ConfigMap) error {
	maps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	current, err := maps.Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.UID != reservation.UID || current.Data["operation_id"] != reservation.Data["operation_id"] {
		return nil
	}
	err = maps.Delete(ctx, OperateLockConfigMap, ownerDeleteOptions(current.ObjectMeta))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// Retire the exact sync Pod, wait for deletion, then release its reservation.
// A cancelled transport cannot delete a replacement launcher's Pod or release
// the PVC while the original Pod still has it mounted.
func (i Installer) finishClaimedSyncPod(cfg Config, reservation *corev1.ConfigMap) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	maps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	current, err := maps.Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.UID != reservation.UID {
		return nil
	}
	if current.Data["owner_kind"] == "Pod" {
		pods := i.Client.CoreV1().Pods(cfg.controlNamespace())
		pod, err := pods.Get(ctx, current.Data["owner_name"], metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil && string(pod.UID) == current.Data["owner_uid"] {
			if err := pods.Delete(ctx, pod.Name, ownerDeleteOptions(pod.ObjectMeta)); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			for {
				remaining, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) || err == nil && remaining.UID != pod.UID {
					break
				}
				if err != nil {
					return err
				}
				if err := i.pause(ctx); err != nil {
					return fmt.Errorf("wait for sync Pod retirement: %w", err)
				}
			}
		}
	}
	return i.releaseOperateReservation(ctx, cfg, reservation)
}
