package operatorcli

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/install"
)

// While the operate Job runs it holds the PVC's operate lock, so the sync Pod cannot start. Its Pod is still a
// read path for the evidence it mounts.
func TestOperateJobPodIsAReadPathForRuntimeEvidence(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sdo-operate-1-abc", Namespace: "sdo-system", Labels: map[string]string{"app.kubernetes.io/name": "sdo-operate"}},
		Spec: corev1.PodSpec{
			Volumes:    []corev1.Volume{{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "repo"}}}},
			Containers: []corev1.Container{{Name: "operate", VolumeMounts: []corev1.VolumeMount{{Name: "repository", MountPath: "/workspace"}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	})
	pod, container, err := sealingController(context.Background(), client, install.SyncConfig{ControlNamespace: "sdo-system", RepositoryPVC: "repo"})
	if err != nil || pod != "sdo-operate-1-abc" || container != "operate" {
		t.Fatalf("running operate Pod = %q/%q, %v; want sdo-operate-1-abc/operate", pod, container, err)
	}
	pod, _, _ = sealingController(context.Background(), client, install.SyncConfig{ControlNamespace: "sdo-system", RepositoryPVC: "other"})
	if pod != "" {
		t.Fatalf("a Pod that does not mount the lane PVC was chosen: %q", pod)
	}
}
