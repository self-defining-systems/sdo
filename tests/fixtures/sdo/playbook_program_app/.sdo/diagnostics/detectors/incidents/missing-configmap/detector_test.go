package missing_configmap

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/sdktest"
)

func TestDetectorGeneralizesAcrossNamesAndIgnoresOptionalReferences(t *testing.T) {
	optional := true
	required := false
	snapshot := sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{
			deployment("renamed-api-v2", "renamed-settings-v7", &required),
			deployment("optional-worker", "optional-settings", &optional),
			deployment("healthy-worker", "present-settings", nil),
		},
		ConfigMapList: []corev1.ConfigMap{
			{ObjectMeta: metav1.ObjectMeta{Name: "present-settings", Namespace: "demo"}},
		},
	}

	findings, err := (Detector{}).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected one missing required ConfigMap, got %#v", findings)
	}
	if got := findings[0].ParameterBindings["target_resource"].Name; got != "renamed-api-v2" {
		t.Fatalf("unexpected target binding %q", got)
	}
	if got := findings[0].ParameterBindings["missing_config_map"].Name; got != "renamed-settings-v7" {
		t.Fatalf("unexpected ConfigMap binding %q", got)
	}
}

func deployment(name string, configMap string, optional *bool) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "app",
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: configMap},
										Optional:             optional,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
