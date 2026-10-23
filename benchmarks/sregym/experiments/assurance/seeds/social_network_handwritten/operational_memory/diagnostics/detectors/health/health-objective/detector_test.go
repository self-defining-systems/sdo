package objective

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

const front = "nginx-thrift"

func frontLabels() map[string]string {
	return map[string]string{"service": "nginx-thrift", "app": "nginx-thrift"}
}

func deployment(name string, replicas int32, available int32) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"service": name}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"service": name}}},
		},
		Status: appsv1.DeploymentStatus{AvailableReplicas: available},
	}
}

// healthy is a snapshot in which every required object is present and ready.
func healthy() sdktest.Snapshot {
	snapshot := sdktest.Snapshot{NamespaceName: "demo"}
	for _, name := range sortedNames(requiredDeployments) {
		snapshot.DeploymentList = append(snapshot.DeploymentList, deployment(name, 1, 1))
	}
	for _, name := range sortedNames(requiredServices) {
		snapshot.ServiceList = append(snapshot.ServiceList, corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"service": name}, Ports: []corev1.ServicePort{{Port: 8080}}},
		})
		snapshot.EndpointList = append(snapshot.EndpointList, corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
			Subsets:    []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}}}},
		})
	}
	for _, name := range sortedNames(requiredConfigMaps) {
		snapshot.ConfigMapList = append(snapshot.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"}})
	}
	return snapshot
}

func detect(t *testing.T, snapshot sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := (Detector{}).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func hasFinding(findings []sdk.Finding, ruleID string, name string) bool {
	for _, finding := range findings {
		if finding.RuleID == ruleID && finding.PrimaryResource.Name == name {
			return true
		}
	}
	return false
}

func TestHealthySnapshotHasNoFindings(t *testing.T) {
	if findings := detect(t, healthy()); len(findings) != 0 {
		t.Fatalf("healthy application reported: %#v", findings)
	}
}

func TestDetectReportsUnavailableRequiredDeploymentAndIgnoresUnrelated(t *testing.T) {
	snapshot := healthy()
	for index := range snapshot.DeploymentList {
		if snapshot.DeploymentList[index].Name == front {
			snapshot.DeploymentList[index].Status.AvailableReplicas = 0
		}
	}
	snapshot.DeploymentList = append(snapshot.DeploymentList, deployment("unrelated-workload", 1, 0))
	findings := detect(t, snapshot)
	if !hasFinding(findings, "deployment-unavailable", front) {
		t.Fatalf("expected %s unavailable: %#v", front, findings)
	}
	if hasFinding(findings, "deployment-unavailable", "unrelated-workload") {
		t.Fatalf("unrelated deployment reported: %#v", findings)
	}
}

func TestDetectReportsScaledToZeroAndMissingDeployment(t *testing.T) {
	snapshot := healthy()
	var kept []appsv1.Deployment
	for _, item := range snapshot.DeploymentList {
		if item.Name == front {
			item.Spec.Replicas = new(int32)
			item.Status.AvailableReplicas = 0
		}
		kept = append(kept, item)
	}
	snapshot.DeploymentList = kept
	findings := detect(t, snapshot)
	if !hasFinding(findings, "deployment-scaled-to-zero", front) || hasFinding(findings, "deployment-unavailable", front) {
		t.Fatalf("expected only scaled-to-zero for %s: %#v", front, findings)
	}
	snapshot.DeploymentList = kept[1:]
	if findings := detect(t, snapshot); !hasFinding(findings, "deployment-missing", kept[0].Name) {
		t.Fatalf("expected %s missing: %#v", kept[0].Name, findings)
	}
}

func TestDetectReportsSelectorMismatch(t *testing.T) {
	snapshot := healthy()
	for index := range snapshot.DeploymentList {
		if snapshot.DeploymentList[index].Name == front {
			snapshot.DeploymentList[index].Spec.Template.Labels = map[string]string{"service": "broken"}
		}
	}
	if findings := detect(t, snapshot); !hasFinding(findings, "deployment-selector-mismatch", front) {
		t.Fatalf("expected selector mismatch: %#v", findings)
	}
}

func TestDetectReportsServiceWithoutEndpointsAndMissingService(t *testing.T) {
	snapshot := healthy()
	snapshot.EndpointList = snapshot.EndpointList[1:]
	missingEndpoints := snapshot.ServiceList[0].Name
	removed := snapshot.ServiceList[1].Name
	snapshot.ServiceList = append(snapshot.ServiceList[:1], snapshot.ServiceList[2:]...)
	findings := detect(t, snapshot)
	if !hasFinding(findings, "service-without-ready-endpoints", missingEndpoints) {
		t.Fatalf("expected %s without endpoints: %#v", missingEndpoints, findings)
	}
	if !hasFinding(findings, "service-missing", removed) {
		t.Fatalf("expected %s missing: %#v", removed, findings)
	}
}

func TestDetectSkipsExternalNameServicesForEndpointReadiness(t *testing.T) {
	snapshot := healthy()
	snapshot.ServiceList[0].Spec = corev1.ServiceSpec{Type: corev1.ServiceTypeExternalName, ExternalName: "alias.example"}
	snapshot.EndpointList = snapshot.EndpointList[1:]
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("ExternalName service reported: %#v", findings)
	}
}

func TestDetectReportsMissingConfigMapsAndIgnoresOptionalReferences(t *testing.T) {
	snapshot := healthy()
	snapshot.ConfigMapList = nil
	for index := range snapshot.DeploymentList {
		if snapshot.DeploymentList[index].Name != front {
			continue
		}
		optional := true
		snapshot.DeploymentList[index].Spec.Template.Spec.Volumes = []corev1.Volume{
			{Name: "required", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "front-required"}}}},
			{Name: "optional", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "front-optional"}, Optional: &optional}}},
		}
	}
	findings := detect(t, snapshot)
	if !hasFinding(findings, "configmap-missing", "nginx-thrift") {
		t.Fatalf("expected nginx-thrift missing: %#v", findings)
	}
	if !hasFinding(findings, "required-configmap-missing", "front-required") {
		t.Fatalf("expected required reference missing: %#v", findings)
	}
	if hasFinding(findings, "required-configmap-missing", "front-optional") {
		t.Fatalf("optional reference reported: %#v", findings)
	}
}

func TestDetectReportsIsolatingNetworkPoliciesAndIgnoresNearMisses(t *testing.T) {
	policy := func(name string, selector metav1.LabelSelector, types []networkingv1.PolicyType, ingress []networkingv1.NetworkPolicyIngressRule) networkingv1.NetworkPolicy {
		return networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
			Spec:       networkingv1.NetworkPolicySpec{PodSelector: selector, PolicyTypes: types, Ingress: ingress},
		}
	}
	both := []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}
	ingressOnly := []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
	allowAll := []networkingv1.NetworkPolicyIngressRule{{}}
	snapshot := healthy()
	snapshot.NetworkPolicyList = []networkingv1.NetworkPolicy{
		policy("isolate-front", metav1.LabelSelector{MatchLabels: frontLabels()}, both, nil),
		policy("deny-ingress-all", metav1.LabelSelector{}, ingressOnly, nil),
		policy("allow-front", metav1.LabelSelector{MatchLabels: frontLabels()}, ingressOnly, allowAll),
		policy("isolate-other", metav1.LabelSelector{MatchLabels: map[string]string{"service": "unrelated-workload"}}, both, nil),
		policy("isolate-expression", metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "service", Operator: metav1.LabelSelectorOpIn, Values: []string{front}}}}, both, nil),
	}
	findings := detect(t, snapshot)
	for rule, name := range map[string]string{
		"network-policy-total-isolation":    "isolate-front",
		"network-policy-denies-all-ingress": "deny-ingress-all",
	} {
		if !hasFinding(findings, rule, name) {
			t.Fatalf("expected %s for %s: %#v", rule, name, findings)
		}
	}
	if !hasFinding(findings, "network-policy-total-isolation", "isolate-expression") {
		t.Fatalf("expected match-expression policy to be evaluated: %#v", findings)
	}
	for _, name := range []string{"allow-front", "isolate-other"} {
		for _, finding := range findings {
			if finding.PrimaryResource.Name == name {
				t.Fatalf("near-miss policy %s reported: %#v", name, finding)
			}
		}
	}
}

func TestRegistration(t *testing.T) {
	spec := New().Spec()
	if spec.ID != "health-objective" || spec.Class != sdk.DetectorClassHealth || len(healthObjectiveDigest) != 64 {
		t.Fatalf("unexpected registration: %#v", spec)
	}
}
