package objective

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"sdo.dev/operator/sdk"
)

const deterministicHealthDetectorVersion = "v3"
const healthObjectiveDigest = "6931b8e687702ada46cc03b5a9c6cb35ccf044dc1e41415b54e2e5092fec9a24"

var requiredDeployments = map[string]struct{}{
	"consul": {},
	"frontend": {},
	"geo": {},
	"jaeger": {},
	"memcached-profile": {},
	"memcached-rate": {},
	"memcached-reserve": {},
	"mongodb-geo": {},
	"mongodb-profile": {},
	"mongodb-rate": {},
	"mongodb-recommendation": {},
	"mongodb-reservation": {},
	"mongodb-user": {},
	"profile": {},
	"rate": {},
	"recommendation": {},
	"reservation": {},
	"search": {},
	"user": {},
}

var requiredServices = map[string]struct{}{
	"consul": {},
	"frontend": {},
	"geo": {},
	"jaeger-out": {},
	"memcached-profile": {},
	"memcached-rate": {},
	"memcached-reserve": {},
	"mongodb-geo": {},
	"mongodb-profile": {},
	"mongodb-rate": {},
	"mongodb-recommendation": {},
	"mongodb-reservation": {},
	"mongodb-user": {},
	"profile": {},
	"rate": {},
	"recommendation": {},
	"reservation": {},
	"search": {},
	"user": {},
}

var requiredWorkloadLabels = []map[string]string{
	{"io.kompose.service": "consul"},
	{"io.kompose.service": "frontend"},
	{"io.kompose.service": "geo"},
	{"io.kompose.service": "jaeger"},
	{"io.kompose.service": "memcached-profile"},
	{"io.kompose.service": "memcached-rate"},
	{"io.kompose.service": "memcached-reserve"},
	{"io.kompose.service": "mongodb-geo"},
	{"io.kompose.service": "mongodb-profile"},
	{"io.kompose.service": "mongodb-rate"},
	{"io.kompose.service": "mongodb-recommendation"},
	{"io.kompose.service": "mongodb-reservation"},
	{"io.kompose.service": "mongodb-user"},
	{"io.kompose.service": "profile"},
	{"io.kompose.service": "rate"},
	{"io.kompose.service": "recommendation"},
	{"io.kompose.service": "reservation"},
	{"io.kompose.service": "search"},
	{"io.kompose.service": "user"},
	{"app-name": "consul", "death-star-project": "hotel-res"},
	{"app-name": "frontend", "death-star-project": "hotel-res"},
	{"app-name": "geo", "death-star-project": "hotel-res"},
	{"app-name": "jaeger", "death-star-project": "hotel-res"},
	{"app-name": "memcached-profile", "death-star-project": "hotel-res"},
	{"app-name": "memcached-rate", "death-star-project": "hotel-res"},
	{"app-name": "memcached-reserve", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-geo", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-profile", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-rate", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-recommendation", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-reservation", "death-star-project": "hotel-res"},
	{"app-name": "mongodb-user", "death-star-project": "hotel-res"},
	{"app-name": "profile", "death-star-project": "hotel-res"},
	{"app-name": "rate", "death-star-project": "hotel-res"},
	{"app-name": "recommendation", "death-star-project": "hotel-res"},
	{"app-name": "reservation", "death-star-project": "hotel-res"},
	{"app-name": "search", "death-star-project": "hotel-res"},
	{"app-name": "user", "death-star-project": "hotel-res"},
}

func isRequiredDeployment(name string) bool {
	_, required := requiredDeployments[name]
	return required
}

func isRequiredService(name string) bool {
	_, required := requiredServices[name]
	return required
}

func policySelectsRequiredWorkload(selector map[string]string) bool {
	for _, labels := range requiredWorkloadLabels {
		matches := true
		for key, value := range selector {
			if labels[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

type Detector struct{}

func New() sdk.Detector { return Detector{} }

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "health-objective", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: 30 * time.Second,
		Watches: []sdk.WatchKind{
			{APIVersion: "v1", Kind: "Pod"},
			{APIVersion: "v1", Kind: "ConfigMap"},
			{APIVersion: "v1", Kind: "Service"},
			{APIVersion: "apps/v1", Kind: "Deployment"},
			{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
			{APIVersion: "v1", Kind: "Endpoints"},
			{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"},
		},
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks: []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
	}
}

func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := make([]sdk.Finding, 0)
	deployments := make(map[string]struct{})
	for _, deployment := range snapshot.Deployments() {
		if deployment.Namespace == snapshot.Namespace() {
			deployments[deployment.Name] = struct{}{}
		}
		for _, ref := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if ref.Optional {
				continue
			}
			if !hasConfigMap(snapshot.ConfigMaps(), deployment.Namespace, ref.Name) {
				findings = append(findings, healthFinding(
					"required-configmap-missing",
					"A required deployment ConfigMap reference is missing",
					fmt.Sprintf("deployment %s/%s requires ConfigMap %s", deployment.Namespace, deployment.Name, ref.Name),
					sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: deployment.Namespace, Name: ref.Name},
				))
			}
		}
		if !isRequiredDeployment(deployment.Name) {
			continue
		}
		if deployment.Spec.Selector == nil || !selectorMatchesLabels(deployment.Spec.Selector.MatchLabels, deployment.Spec.Template.Labels) {
			findings = append(findings, healthFinding(
				"deployment-selector-mismatch",
				"A required deployment selector does not match its pod template labels",
				fmt.Sprintf("deployment %s/%s selector does not match template labels", deployment.Namespace, deployment.Name),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
		}
		desired := int32(1)
		if deployment.Spec.Replicas != nil {
			desired = *deployment.Spec.Replicas
		}
		if desired == 0 {
			findings = append(findings, healthFinding(
				"deployment-scaled-to-zero",
				"A required deployment has no desired replicas",
				fmt.Sprintf("deployment %s/%s is explicitly scaled to zero", deployment.Namespace, deployment.Name),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
			continue
		}
		if deployment.Status.AvailableReplicas < desired {
			findings = append(findings, healthFinding(
				"deployment-unavailable",
				"A required deployment has unavailable replicas",
				fmt.Sprintf(
					"deployment %s/%s has %d/%d available replicas",
					deployment.Namespace,
					deployment.Name,
					deployment.Status.AvailableReplicas,
					desired,
				),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
		}
	}
	for _, name := range sortedNames(requiredDeployments) {
		if _, ok := deployments[name]; !ok {
			findings = append(findings, healthFinding("deployment-missing", "A required deployment is missing", fmt.Sprintf("deployment %s/%s is absent", snapshot.Namespace(), name), sdk.ObjectRef{APIVersion:"apps/v1", Kind:"Deployment", Namespace:snapshot.Namespace(), Name:name}))
		}
	}
	services := make(map[string]struct{})
	for _, service := range snapshot.Services() {
		if service.Namespace == snapshot.Namespace() { services[service.Name] = struct{}{} }
		if !isRequiredService(service.Name) {
			continue
		}
		if !sdk.ServiceExpectsEndpoints(service) {
			continue
		}
		if snapshot.ReadyEndpointCountForService(service.Namespace, service.Name) == 0 {
			findings = append(findings, healthFinding(
				"service-without-ready-endpoints",
				"A selected service has no ready endpoints",
				fmt.Sprintf("service %s/%s has zero ready endpoints", service.Namespace, service.Name),
				sdk.ObjectRefFrom("Service", "v1", &service),
			))
		}
	}
	for _, name := range sortedNames(requiredServices) {
		if _, ok := services[name]; !ok {
			findings = append(findings, healthFinding("service-missing", "A required service is missing", fmt.Sprintf("service %s/%s is absent", snapshot.Namespace(), name), sdk.ObjectRef{APIVersion:"v1", Kind:"Service", Namespace:snapshot.Namespace(), Name:name}))
		}
	}
	for _, policy := range snapshot.NetworkPolicies() {
		if !policySelectsRequiredWorkload(policy.Spec.PodSelector.MatchLabels) {
			continue
		}
		isolatesIngress := false
		isolatesEgress := false
		for _, policyType := range policy.Spec.PolicyTypes {
			isolatesIngress = isolatesIngress || policyType == networkingv1.PolicyTypeIngress
			isolatesEgress = isolatesEgress || policyType == networkingv1.PolicyTypeEgress
		}
		if isolatesIngress && len(policy.Spec.Ingress) == 0 && isolatesEgress && len(policy.Spec.Egress) == 0 {
			findings = append(findings, healthFinding(
				"network-policy-total-isolation",
				"A network policy completely isolates selected application pods",
				fmt.Sprintf("networkpolicy %s/%s denies all ingress and egress", policy.Namespace, policy.Name),
				sdk.ObjectRefFrom("NetworkPolicy", "networking.k8s.io/v1", &policy),
			))
		}
	}
	return findings, nil
}

func selectorMatchesLabels(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func hasConfigMap(configMaps []corev1.ConfigMap, namespace, name string) bool {
	for _, cm := range configMaps { if cm.Namespace == namespace && cm.Name == name { return true } }
	return false
}

func sortedNames(values map[string]struct{}) []string {
	names := make([]string, 0, len(values))
	for name := range values { names = append(names, name) }
	sort.Strings(names)
	return names
}

func healthFinding(ruleID string, summary string, evidence string, resource sdk.ObjectRef) sdk.Finding {
	return sdk.Finding{
		DetectorID: "health-objective", RuleID: ruleID, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: summary, Evidence: evidence,
		PrimaryResource: resource,
		Playbooks: []string{"health-objective"},
		Fingerprint: "health-objective/" + ruleID + "/" + resource.Namespace + "/" + resource.Name,
	}
}
