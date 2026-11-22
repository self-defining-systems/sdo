package install

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// A controller whose Pod exists but is not ready names why: "no ready owned Pod" alone cost a lane's retry to diagnose,
// while the Pod's scheduling condition and image-pull state say it at once (a full node, a volume pinned to another one,
// a 3 GB image still being pulled).
func TestInspectExistingNamesWhyTheControllerPodIsNotReady(t *testing.T) {
	cfg := InspectConfig{ControlNamespace: "sdo-system", Namespace: "app", Application: "app", RepositoryPVC: "repository", ControllerImage: "sdo-controller:test"}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cfg.ControlNamespace, Name: ControllerJobName, UID: types.UID("job-uid"),
			Annotations: map[string]string{InstallFingerprintAnnotation: "fp", WorkerMountPolicyAnnotation: "proposal-only"},
		},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "controller", Image: cfg.ControllerImage, Command: []string{"/usr/local/bin/sdo-operator"},
				Args: []string{"--broker-mode=native", "--worker-mount-mode=proposal-only", "--namespace", cfg.Namespace, "--application", cfg.Application},
			}},
			Volumes: []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.RepositoryPVC}}}},
		}}},
		Status: batchv1.JobStatus{Active: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cfg.ControlNamespace, Name: "sdo-controller-abc",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: ControllerJobName, UID: job.UID}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable",
				Message: "0/2 nodes are available: 1 Insufficient ephemeral-storage, 1 node(s) didn't match PersistentVolume's node affinity.",
			}},
		},
	}
	installer := Installer{
		Client:             fake.NewSimpleClientset(job, pod),
		ReadAcceptedCommit: func(context.Context, SyncConfig) (string, error) { return strings.Repeat("a", 40), nil },
	}
	_, err := installer.InspectExisting(context.Background(), cfg)
	if err == nil {
		t.Fatal("a pending controller Pod was accepted")
	}
	for _, want := range []string{"existing controller has no ready owned Pod", "sdo-controller-abc", "Pending", "Insufficient ephemeral-storage"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}
