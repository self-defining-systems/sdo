package install

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSyncPodCarriesTheInstallsCredentialsAndOperator(t *testing.T) {
	cfg := policyConfig()
	job := controllerJob(cfg)
	spec := &job.Spec.Template.Spec
	spec.Volumes = append(spec.Volumes,
		corev1.Volume{Name: "claude-accounts", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "accounts"}}},
		corev1.Volume{Name: "dev-operator", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev-tree/sdo-operator"}}})
	spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "dev-operator", MountPath: operatorBinary})
	control := job.Namespace
	secret := func(name string) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: control}}
	}
	for _, present := range []bool{true, false} {
		client := fake.NewSimpleClientset(job, secret("accounts"))
		if present {
			client = fake.NewSimpleClientset(job, secret("accounts"), secret(cfg.CredentialsSecret))
		}
		pod := syncPod(cfg)
		if err := (Installer{Client: client}).projectInstall(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			got[mount.MountPath] = mount.ReadOnly
		}
		if got["/sdo/credentials"] != present || !got["/sdo/claude-accounts"] || !got[operatorBinary] {
			t.Errorf("Secret present=%v: read-only mounts %v", present, got)
		}
	}
}
