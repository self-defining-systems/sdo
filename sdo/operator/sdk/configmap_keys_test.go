package sdk

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestConfigMapKeyReferencesForDeployment(t *testing.T) {
	optional := true
	d := appsv1.Deployment{}
	d.Spec.Template.Spec = corev1.PodSpec{
		Containers: []corev1.Container{{Env: []corev1.EnvVar{
			{ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "endpoint", Optional: &optional}}},
			{ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "private"}, Key: "password"}}},
		}, EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "all-keys"}}}}}},
		InitContainers: []corev1.Container{{Env: []corev1.EnvVar{{ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "endpoint"}}}}}},
		Volumes: []corev1.Volume{
			{VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Items: []corev1.KeyToPath{{Key: "script", Path: "boot.sh"}}}}},
			{VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "assets"}, Optional: &optional, Items: []corev1.KeyToPath{{Key: "logo", Path: "logo"}}}}}}}},
		},
	}
	want := []ConfigMapKeyReference{{Name: "assets", Key: "logo", Optional: true}, {Name: "settings", Key: "endpoint"}, {Name: "settings", Key: "script"}}
	if got := ConfigMapKeyReferencesForDeployment(d); !reflect.DeepEqual(got, want) {
		t.Fatalf("key references: got %#v, want %#v", got, want)
	}
	if got := ConfigMapKeyReferencesForDeployment(appsv1.Deployment{}); len(got) != 0 {
		t.Fatalf("empty template has references: %#v", got)
	}
}
