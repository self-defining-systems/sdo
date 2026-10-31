package sdk

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPodTemplateReferencesPreserveKeysAndOptional(t *testing.T) {
	optional := true
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "worker", ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry"}}, Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}, Containers: []corev1.Container{{Env: []corev1.EnvVar{{ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "password", Optional: &optional}}}}}}}}
	refs := PodTemplateReferences(template)
	if len(refs) != 4 {
		t.Fatalf("references: %#v", refs)
	}
	found := false
	for _, r := range refs {
		if r.Kind == "Secret" && r.Name == "credentials" && r.Key == "password" && r.Optional {
			found = true
		}
	}
	if !found {
		t.Fatalf("secret key reference missing: %#v", refs)
	}
}

func TestPodTemplateReferencesCoverSecretFormsAndRequiredWins(t *testing.T) {
	optional := true
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Volumes: []corev1.Volume{
			{VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "volume", Optional: &optional, Items: []corev1.KeyToPath{{Key: "key", Path: "file"}}}}},
			{VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "projected"}, Optional: &optional, Items: []corev1.KeyToPath{{Key: "key", Path: "file"}}}}, {ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "config"}, Optional: &optional, Items: []corev1.KeyToPath{{Key: "setting", Path: "file"}}}}}}}},
		},
		InitContainers:      []corev1.Container{{EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "volume"}}}}}},
		EphemeralContainers: []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Env: []corev1.EnvVar{{ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "projected"}, Key: "key"}}}}}}},
	}}
	want := []PodTemplateReference{
		{Kind: "ConfigMap", Name: "config", Optional: true},
		{Kind: "ConfigMap", Name: "config", Key: "setting", Optional: true},
		{Kind: "Secret", Name: "projected", Optional: true},
		{Kind: "Secret", Name: "projected", Key: "key"},
		{Kind: "Secret", Name: "volume"},
		{Kind: "Secret", Name: "volume", Key: "key", Optional: true},
		{Kind: "ServiceAccount", Name: "default"},
	}
	got := PodTemplateReferences(template)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("references:\n got %#v\nwant %#v", got, want)
	}
}
