package install

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInspectExistingRequiresCurrentReadyNativeControllerAndAcceptedHead(t *testing.T) {
	ctx := context.Background()
	jobUID := "current-job"
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: ControllerJobName, Namespace: "demo-sdo", UID: "current-job", Annotations: map[string]string{InstallFingerprintAnnotation: "fingerprint", WorkerMountPolicyAnnotation: "proposal-only"}}, Status: batchv1.JobStatus{Active: 1}}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "controller", Image: "controller:go", Command: []string{"/usr/local/bin/sdo-operator"}, Args: []string{"controller", "--namespace", "demo", "--application", "demo", "--broker-mode=native", "--worker-mount-mode=proposal-only"}}}
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "repository"}}}}
	client := fake.NewSimpleClientset(
		job,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller-pod", Namespace: "demo-sdo", UID: "current-pod", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: ControllerJobName, UID: "current-job"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: "demo-sdo", UID: "old-pod", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: ControllerJobName, UID: "old-job"}}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
	)
	installer := Installer{Client: client, ReadAcceptedCommit: func(context.Context, SyncConfig) (string, error) { return strings.Repeat("a", 40), nil }}
	result, err := installer.InspectExisting(ctx, InspectConfig{ControlNamespace: "demo-sdo", Namespace: "demo", Application: "demo", RepositoryPVC: "repository", ControllerImage: "controller:go"})
	if err != nil {
		t.Fatal(err)
	}
	if result.JobUID != jobUID || result.PodUID != "current-pod" || result.Fingerprint != "fingerprint" || result.AcceptedCommit != strings.Repeat("a", 40) {
		t.Fatalf("unexpected inspected controller: %+v", result)
	}
	if _, err := installer.InspectExisting(ctx, InspectConfig{ControlNamespace: "demo-sdo", Namespace: "demo", Application: "demo", RepositoryPVC: "repository", ControllerImage: "wrong:image"}); err == nil {
		t.Fatal("accepted mismatched controller image")
	}
}

// The benchmark recreates the application namespace between stages, which deletes the controller's grants
// there; inspecting the persistent controller must restore them or it can no longer observe the application.
func TestInspectExistingRestoresApplicationGrantsAfterNamespaceRecreation(t *testing.T) {
	ctx := context.Background()
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: ControllerJobName, Namespace: "demo-sdo", UID: "current-job", Annotations: map[string]string{InstallFingerprintAnnotation: "fingerprint", WorkerMountPolicyAnnotation: "proposal-only"}}, Status: batchv1.JobStatus{Active: 1}}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "controller", Image: "controller:go", Command: []string{"/usr/local/bin/sdo-operator"}, Args: []string{"controller", "--namespace", "demo", "--application", "demo", "--broker-mode=native", "--worker-mount-mode=proposal-only"}}}
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "repository"}}}}
	client := fake.NewSimpleClientset(
		job,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller-pod", Namespace: "demo-sdo", UID: "current-pod", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: ControllerJobName, UID: "current-job"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	)
	installer := Installer{Client: client, ReadAcceptedCommit: func(context.Context, SyncConfig) (string, error) { return strings.Repeat("a", 40), nil }}
	if _, err := installer.InspectExisting(ctx, InspectConfig{ControlNamespace: "demo-sdo", Namespace: "demo", Application: "demo", RepositoryPVC: "repository", ControllerImage: "controller:go"}); err != nil {
		t.Fatal(err)
	}
	roles, err := client.RbacV1().Roles("demo").List(ctx, metav1.ListOptions{})
	if err != nil || len(roles.Items) == 0 {
		t.Fatalf("application grants were not restored: %v, %d roles", err, len(roles.Items))
	}
	bindings, err := client.RbacV1().RoleBindings("demo").List(ctx, metav1.ListOptions{})
	if err != nil || len(bindings.Items) == 0 {
		t.Fatalf("application bindings were not restored: %v, %d bindings", err, len(bindings.Items))
	}
}
