package objective

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestDetectReportsUnavailableRequiredDeploymentAndIgnoresNearMiss(t *testing.T) {
	replicas := int32(1)
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{
			{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "demo"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}},
			{ObjectMeta: metav1.ObjectMeta{Name: "unrelated-workload", Namespace: "demo"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(findings, "deployment-unavailable", "frontend") {
		t.Fatalf("expected frontend unavailable: %#v", findings)
	}
	if hasFinding(findings, "deployment-unavailable", "unrelated-workload") {
		t.Fatalf("unrelated deployment reported: %#v", findings)
	}
}

func TestDetectReportsRequiredDeploymentScaledToZeroAndIgnoresNearMiss(t *testing.T) {
	zero := int32(0)
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{
			{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "demo"}, Spec: appsv1.DeploymentSpec{Replicas: &zero}},
			{ObjectMeta: metav1.ObjectMeta{Name: "unrelated-workload", Namespace: "demo"}, Spec: appsv1.DeploymentSpec{Replicas: &zero}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(findings, "deployment-scaled-to-zero", "frontend") {
		t.Fatalf("expected frontend scaled-to-zero: %#v", findings)
	}
	if hasFinding(findings, "deployment-scaled-to-zero", "unrelated-workload") {
		t.Fatalf("unrelated deployment reported: %#v", findings)
	}
}

func TestDetectAcceptsRequiredDeploymentWithPositiveReplicas(t *testing.T) {
	one := int32(1)
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{{
			ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "demo"},
			Spec:       appsv1.DeploymentSpec{Replicas: &one},
			Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(findings, "deployment-scaled-to-zero", "frontend") {
		t.Fatalf("positive replica deployment reported: %#v", findings)
	}
}

func TestDetectReportsRequiredDeploymentSelectorMismatchAndIgnoresNearMiss(t *testing.T) {
	replicas := int32(1)
	deployment := func(name string, selector, labels map[string]string) appsv1.Deployment {
		return appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: selector},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}},
			},
			Status: appsv1.DeploymentStatus{AvailableReplicas: 1},
		}
	}
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{
			deployment("frontend", map[string]string{"app": "frontend"}, map[string]string{"app": "broken"}),
			deployment("unrelated-workload", map[string]string{"app": "other"}, map[string]string{"app": "mismatch"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(findings, "deployment-selector-mismatch", "frontend") {
		t.Fatalf("expected frontend selector mismatch: %#v", findings)
	}
	if hasFinding(findings, "deployment-selector-mismatch", "unrelated-workload") {
		t.Fatalf("unrelated deployment reported: %#v", findings)
	}
}

func TestDetectAcceptsMatchingRequiredDeploymentSelector(t *testing.T) {
	replicas := int32(1)
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{{
			ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "demo"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "frontend"}},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "frontend", "tier": "web"}}},
			},
			Status: appsv1.DeploymentStatus{AvailableReplicas: 1},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(findings, "deployment-selector-mismatch", "frontend") {
		t.Fatalf("matching selector reported: %#v", findings)
	}
}

func TestDetectSkipsExternalNameAliasesForEndpointReadiness(t *testing.T) {
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
		NamespaceName: "demo",
		ServiceList:   []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "jaeger", Namespace: "demo"}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: "jaeger.example"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(findings, "service-without-ready-endpoints", "jaeger") {
		t.Fatalf("ExternalName alias reported for endpoints: %#v", findings)
	}
}

func TestDetectRequiresNonOptionalConfigMapReferences(t *testing.T) {
	replicas := int32(1)
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "demo"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "required", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "needed"}}}}, {Name: "optional", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "optional-cm"}, Optional: boolPtr(true)}}}}}}},
		Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
	}
	findings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{deployment}})
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(findings, "required-configmap-missing", "needed") {
		t.Fatalf("required ConfigMap absence not reported: %#v", findings)
	}
	if hasFinding(findings, "required-configmap-missing", "optional-cm") {
		t.Fatalf("optional ConfigMap reported: %#v", findings)
	}
	findings, err = (Detector{}).Detect(context.Background(), sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{deployment}, ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "needed", Namespace: "demo"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if hasFinding(findings, "required-configmap-missing", "needed") {
		t.Fatalf("present ConfigMap reported missing: %#v", findings)
	}
}

func boolPtr(value bool) *bool { return &value }

func hasFinding(findings []sdk.Finding, rule, name string) bool {
	for _, finding := range findings {
		if finding.RuleID == rule && finding.PrimaryResource.Name == name {
			return true
		}
	}
	return false
}
