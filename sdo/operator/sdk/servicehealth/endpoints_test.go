package servicehealth_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

func int32Ptr(v int32) *int32 { return &v }

func scaledSnapshot(replicas int32, hpas ...autoscalingv2.HorizontalPodAutoscaler) sdktest.Snapshot {
	labels := map[string]string{"app": "api"}
	return sdktest.Snapshot{
		NamespaceName: "app",
		ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", Selector: labels, Ports: []corev1.ServicePort{{Port: 80}}}}},
		DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
			Spec: appsv1.DeploymentSpec{Replicas: int32Ptr(replicas),
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}}},
		HPAList: hpas,
	}
}

func detectEndpoints(t *testing.T, snapshot sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{ID: "service-endpoints"}).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// The live replica count is what a scale-to-zero fault changes, so it cannot
// say the workload was meant to be off: a routed Service with no endpoint is a
// finding at zero replicas too.
func TestServiceWithScaledToZeroBackendIsAFinding(t *testing.T) {
	findings := detectEndpoints(t, scaledSnapshot(0))
	if len(findings) != 1 || findings[0].RuleID != servicehealth.RuleNoReadyEndpoints {
		t.Fatalf("scaled-to-zero backend findings = %+v, want one %s", findings, servicehealth.RuleNoReadyEndpoints)
	}
	if findings[0].Evidence == "" || !contains(findings[0].Evidence, "0 replicas") {
		t.Fatalf("evidence must name the zero replica count: %q", findings[0].Evidence)
	}
}

func TestServiceWithScaledToZeroStatefulSetIsAFinding(t *testing.T) {
	snapshot := scaledSnapshot(0)
	labels := map[string]string{"app": "api"}
	snapshot.DeploymentList = nil
	snapshot.StatefulSetList = []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
		Spec: appsv1.StatefulSetSpec{Replicas: int32Ptr(0), Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}}}
	findings := detectEndpoints(t, snapshot)
	if len(findings) != 1 || findings[0].RuleID != servicehealth.RuleNoReadyEndpoints {
		t.Fatalf("scaled-to-zero StatefulSet findings = %+v, want one %s", findings, servicehealth.RuleNoReadyEndpoints)
	}
}

// An autoscaler allowed to reach zero declares that off is a normal state.
func TestAutoscalerThatAllowsZeroDeclaresTheWorkloadMayBeOff(t *testing.T) {
	hpa := autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: int32Ptr(0),
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "api"}}}
	if findings := detectEndpoints(t, scaledSnapshot(0, hpa)); len(findings) != 0 {
		t.Fatalf("zero-allowed autoscaled workload findings = %+v, want none", findings)
	}
	hpa.Spec.MinReplicas = int32Ptr(1)
	if findings := detectEndpoints(t, scaledSnapshot(0, hpa)); len(findings) != 1 {
		t.Fatalf("autoscaler with minReplicas 1 must not excuse zero replicas: %+v", findings)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
