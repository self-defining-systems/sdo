package sdktest

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

func TestCaptureSnapshotBoundsNamespaceAndRedactsEnvironment(t *testing.T) {
	view := Snapshot{NamespaceName: "app", PodList: []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "API_TOKEN", Value: "private"}, {Name: "MODE", Value: "live"}}}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "elsewhere"}},
	}, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "private"}}}}}}}}}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.PodList) != 1 || captured.PodList[0].Name != "broken" || captured.PodList[0].Spec.Containers[0].Env[0].Value != "" || captured.PodList[0].Spec.Containers[0].Env[1].Value != "live" || captured.DeploymentList[0].Spec.Template.Spec.Containers[0].Env[0].Value != "" {
		t.Fatalf("snapshot did not preserve scoped, redacted state: %+v", captured)
	}
	if view.PodList[0].Spec.Containers[0].Env[0].Value != "private" {
		t.Fatal("capture mutated live view")
	}
	if _, err := CaptureSnapshot(view, "other"); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("mismatched namespace accepted: %v", err)
	}
	var _ sdk.DetectionContext = captured
}

func TestCaptureSnapshotDropsKubernetesWriteMetadata(t *testing.T) {
	view := Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{
		Name: "api", Namespace: "app", ResourceVersion: "123", UID: "opaque", Generation: 7,
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: strings.Repeat("manager", 1000)}},
		Annotations:   map[string]string{"kubectl.kubernetes.io/last-applied-configuration": strings.Repeat("manifest", 1000), "kept": "value"},
	}}}}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	got := captured.PodList[0].ObjectMeta
	if got.ResourceVersion != "" || got.UID != "" || got.Generation != 0 || len(got.ManagedFields) != 0 || got.Annotations["kubectl.kubernetes.io/last-applied-configuration"] != "" || got.Annotations["kept"] != "value" {
		t.Fatalf("capture metadata: %+v", got)
	}
	if view.PodList[0].ResourceVersion != "123" {
		t.Fatal("capture changed live metadata")
	}
}
