package servicehealth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

const namespace = "hotel-reservation"

func spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "service-endpoints", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:     servicehealth.EndpointWatches(),
		Interval:    15 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:   []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

func service(name string, selector map[string]string) corev1.Service {
	return corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports:    []corev1.ServicePort{{Name: "http", Port: 5000}},
		},
	}
}

func deployment(name string, replicas int32, labels map[string]string) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}},
		},
	}
}

func pod(name string, labels map[string]string, ready bool) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func endpoints(name string, ready int) corev1.Endpoints {
	subset := corev1.EndpointSubset{}
	for index := 0; index < ready; index++ {
		subset.Addresses = append(subset.Addresses, corev1.EndpointAddress{IP: "10.244.0.10"})
	}
	return corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Subsets:    []corev1.EndpointSubset{subset},
	}
}

func decoys() []corev1.ConfigMap {
	return []corev1.ConfigMap{
		{ObjectMeta: metav1.ObjectMeta{Name: "failure-admin-geo", Namespace: namespace},
			Data: map[string]string{"revoke-admin-geo-mongo.sh": "db.revokeRolesFromUser('admin', ['readWrite'])"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "failure-admin-rate", Namespace: namespace},
			Data: map[string]string{"revoke-admin-rate-mongo.sh": "db.revokeRolesFromUser('admin', ['readWrite'])"}},
	}
}

var frontendLabels = map[string]string{"io.kompose.service": "frontend"}

func healthyHotel() sdktest.Snapshot {
	return sdktest.Snapshot{
		NamespaceName:  namespace,
		ConfigMapList:  decoys(),
		ServiceList:    []corev1.Service{service("frontend", frontendLabels)},
		DeploymentList: []appsv1.Deployment{deployment("frontend", 1, frontendLabels)},
		PodList:        []corev1.Pod{pod("frontend-7d9", frontendLabels, true)},
		EndpointList:   []corev1.Endpoints{endpoints("frontend", 1)},
	}
}

func detect(t *testing.T, snapshot sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := servicehealth.NewReadyEndpointsDetector(spec()).Detect(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	return findings
}

func TestNoFindingForHealthyServicesWithDecoysPresent(t *testing.T) {
	if findings := detect(t, healthyHotel()); len(findings) != 0 {
		t.Fatalf("healthy app with decoy ConfigMaps must not fire, got %v", findings)
	}
}

func TestWrongSelectorNamesTheMismatchedLabel(t *testing.T) {
	snapshot := healthyHotel()
	snapshot.ServiceList = []corev1.Service{service("frontend", map[string]string{
		"io.kompose.service": "frontend", "current_service_name": "frontend",
	})}
	snapshot.EndpointList = []corev1.Endpoints{endpoints("frontend", 0)}
	findings := detect(t, snapshot)
	if len(findings) != 1 {
		t.Fatalf("expected one finding, got %v", findings)
	}
	finding := findings[0]
	if finding.RuleID != "service-selector-matches-no-pods" ||
		finding.PrimaryResource != (sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: "frontend"}) {
		t.Fatalf("finding must point at the Service selector, got %+v", finding)
	}
	for _, want := range []string{"current_service_name=frontend", "Deployment frontend"} {
		if !strings.Contains(finding.Evidence, want) {
			t.Fatalf("evidence %q must name %q", finding.Evidence, want)
		}
	}
	for _, decoy := range []string{"failure-admin-geo", "failure-admin-rate"} {
		if strings.Contains(finding.Summary+finding.Evidence, decoy) {
			t.Fatalf("finding must not mention decoy %s: %+v", decoy, finding)
		}
	}
	if len(finding.RelatedResources) != 1 || finding.RelatedResources[0].Name != "frontend" ||
		finding.RelatedResources[0].Kind != "Deployment" {
		t.Fatalf("finding must relate the workload the selector was meant to match, got %v", finding.RelatedResources)
	}
}

func TestSelectedPodsThatAreNotReadyFire(t *testing.T) {
	snapshot := healthyHotel()
	snapshot.PodList = []corev1.Pod{pod("frontend-7d9", frontendLabels, false)}
	snapshot.EndpointList = []corev1.Endpoints{endpoints("frontend", 0)}
	findings := detect(t, snapshot)
	if len(findings) != 1 || findings[0].RuleID != "service-has-no-ready-endpoints" ||
		!strings.Contains(findings[0].Evidence, "frontend-7d9") {
		t.Fatalf("expected a no-ready-endpoints finding naming the unready pod, got %v", findings)
	}
}

func TestIntentionallyScaledToZeroDoesNotFire(t *testing.T) {
	snapshot := healthyHotel()
	snapshot.DeploymentList = []appsv1.Deployment{deployment("frontend", 0, frontendLabels)}
	snapshot.PodList = nil
	snapshot.EndpointList = []corev1.Endpoints{endpoints("frontend", 0)}
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("a Service whose only workload is scaled to zero is not a fault, got %v", findings)
	}
}

func TestServicesWithoutSelectorAreSkipped(t *testing.T) {
	snapshot := healthyHotel()
	snapshot.ServiceList = append(snapshot.ServiceList, service("external-db", nil))
	if findings := detect(t, snapshot); len(findings) != 0 {
		t.Fatalf("a selector-less Service has manually managed endpoints, got %v", findings)
	}
}

func TestExternalNameServicesNeverFire(t *testing.T) {
	violations := sdktest.ExternalNameEndpointViolations(
		servicehealth.NewReadyEndpointsDetector(spec()), []string{"frontend", "search"},
	)
	if len(violations) != 0 {
		t.Fatalf("detector must exempt ExternalName Services: %v", violations)
	}
}
