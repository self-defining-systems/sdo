// Package objective checks the human health objective against the deployed
// Kubernetes objects of the application. It is purely observational: it names
// the required objects the application's own manifests deploy and reports when
// one is missing, scaled to zero, unavailable, without ready endpoints, or
// isolated by a NetworkPolicy. It encodes no knowledge of any particular fault.
package objective

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
)

const healthObjectiveDigest = "ed9bf87d40e14ebc6edfc61a83723bd12daa840eab09723841dad20b549a3ffe"

var requiredDeployments = map[string]struct{}{
	"accounting":      {},
	"ad":              {},
	"cart":            {},
	"checkout":        {},
	"currency":        {},
	"email":           {},
	"flagd":           {},
	"fraud-detection": {},
	"frontend":        {},
	"frontend-proxy":  {},
	"image-provider":  {},
	"kafka":           {},
	"llm":             {},
	"payment":         {},
	"postgresql":      {},
	"product-catalog": {},
	"product-reviews": {},
	"quote":           {},
	"recommendation":  {},
	"shipping":        {},
	"valkey-cart":     {},
}

var requiredServices = map[string]struct{}{
	"ad":              {},
	"cart":            {},
	"checkout":        {},
	"currency":        {},
	"email":           {},
	"flagd":           {},
	"frontend":        {},
	"frontend-proxy":  {},
	"image-provider":  {},
	"kafka":           {},
	"llm":             {},
	"payment":         {},
	"postgresql":      {},
	"product-catalog": {},
	"product-reviews": {},
	"quote":           {},
	"recommendation":  {},
	"shipping":        {},
	"valkey-cart":     {},
}

var requiredConfigMaps = map[string]struct{}{
	"flagd-config":    {},
	"postgresql-init": {},
}

// requiredWorkloadLabels are the pod template labels of the required
// Deployments; a NetworkPolicy that selects any of them can isolate the
// application.
var requiredWorkloadLabels = []map[string]string{
	{"opentelemetry.io/name": "accounting", "app.kubernetes.io/component": "accounting", "app.kubernetes.io/name": "accounting"},
	{"opentelemetry.io/name": "ad", "app.kubernetes.io/component": "ad", "app.kubernetes.io/name": "ad"},
	{"opentelemetry.io/name": "cart", "app.kubernetes.io/component": "cart", "app.kubernetes.io/name": "cart"},
	{"opentelemetry.io/name": "checkout", "app.kubernetes.io/component": "checkout", "app.kubernetes.io/name": "checkout"},
	{"opentelemetry.io/name": "currency", "app.kubernetes.io/component": "currency", "app.kubernetes.io/name": "currency"},
	{"opentelemetry.io/name": "email", "app.kubernetes.io/component": "email", "app.kubernetes.io/name": "email"},
	{"opentelemetry.io/name": "flagd", "app.kubernetes.io/component": "flagd", "app.kubernetes.io/name": "flagd"},
	{"opentelemetry.io/name": "fraud-detection", "app.kubernetes.io/component": "fraud-detection", "app.kubernetes.io/name": "fraud-detection"},
	{"opentelemetry.io/name": "frontend", "app.kubernetes.io/component": "frontend", "app.kubernetes.io/name": "frontend"},
	{"opentelemetry.io/name": "frontend-proxy", "app.kubernetes.io/component": "frontend-proxy", "app.kubernetes.io/name": "frontend-proxy"},
	{"opentelemetry.io/name": "image-provider", "app.kubernetes.io/component": "image-provider", "app.kubernetes.io/name": "image-provider"},
	{"opentelemetry.io/name": "kafka", "app.kubernetes.io/component": "kafka", "app.kubernetes.io/name": "kafka"},
	{"opentelemetry.io/name": "llm", "app.kubernetes.io/component": "llm", "app.kubernetes.io/name": "llm"},
	{"opentelemetry.io/name": "payment", "app.kubernetes.io/component": "payment", "app.kubernetes.io/name": "payment"},
	{"opentelemetry.io/name": "postgresql", "app.kubernetes.io/component": "postgresql", "app.kubernetes.io/name": "postgresql"},
	{"opentelemetry.io/name": "product-catalog", "app.kubernetes.io/component": "product-catalog", "app.kubernetes.io/name": "product-catalog"},
	{"opentelemetry.io/name": "product-reviews", "app.kubernetes.io/component": "product-reviews", "app.kubernetes.io/name": "product-reviews"},
	{"opentelemetry.io/name": "quote", "app.kubernetes.io/component": "quote", "app.kubernetes.io/name": "quote"},
	{"opentelemetry.io/name": "recommendation", "app.kubernetes.io/component": "recommendation", "app.kubernetes.io/name": "recommendation"},
	{"opentelemetry.io/name": "shipping", "app.kubernetes.io/component": "shipping", "app.kubernetes.io/name": "shipping"},
	{"opentelemetry.io/name": "valkey-cart", "app.kubernetes.io/component": "valkey-cart", "app.kubernetes.io/name": "valkey-cart"},
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
			{APIVersion: "apps/v1", Kind: "ReplicaSet"},
			{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
			{APIVersion: "v1", Kind: "Endpoints"},
			{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"},
		},
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks:         []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
	}
}

func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	namespace := snapshot.Namespace()
	findings := make([]sdk.Finding, 0)
	deployments := make(map[string]struct{})
	for _, deployment := range snapshot.Deployments() {
		if deployment.Namespace != namespace {
			continue
		}
		deployments[deployment.Name] = struct{}{}
		for _, ref := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if ref.Optional || hasConfigMap(snapshot.ConfigMaps(), deployment.Namespace, ref.Name) {
				continue
			}
			findings = append(findings, healthFinding(
				"required-configmap-missing",
				"A required deployment ConfigMap reference is missing",
				fmt.Sprintf("deployment %s/%s requires ConfigMap %s", deployment.Namespace, deployment.Name, ref.Name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: deployment.Namespace, Name: ref.Name},
			))
		}
		if _, required := requiredDeployments[deployment.Name]; !required {
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
				fmt.Sprintf("deployment %s/%s has %d/%d available replicas",
					deployment.Namespace, deployment.Name, deployment.Status.AvailableReplicas, desired),
				sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment),
			))
		}
	}
	for _, name := range sortedNames(requiredDeployments) {
		if _, ok := deployments[name]; !ok {
			findings = append(findings, healthFinding(
				"deployment-missing", "A required deployment is missing",
				fmt.Sprintf("deployment %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: name},
			))
		}
	}
	services := make(map[string]struct{})
	for _, service := range snapshot.Services() {
		if service.Namespace != namespace {
			continue
		}
		services[service.Name] = struct{}{}
		if _, required := requiredServices[service.Name]; !required || !sdk.ServiceExpectsEndpoints(service) {
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
			findings = append(findings, healthFinding(
				"service-missing", "A required service is missing",
				fmt.Sprintf("service %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: name},
			))
		}
	}
	for _, name := range sortedNames(requiredConfigMaps) {
		if !hasConfigMap(snapshot.ConfigMaps(), namespace, name) {
			findings = append(findings, healthFinding(
				"configmap-missing", "A required ConfigMap is missing",
				fmt.Sprintf("configmap %s/%s is absent", namespace, name),
				sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: name},
			))
		}
	}
	for _, policy := range snapshot.NetworkPolicies() {
		if policy.Namespace != namespace || !policySelectsRequiredWorkload(policy.Spec.PodSelector) {
			continue
		}
		ruleID, summary, evidence := policyIsolation(policy)
		if ruleID == "" {
			continue
		}
		findings = append(findings, healthFinding(
			ruleID, summary,
			fmt.Sprintf("networkpolicy %s/%s %s", policy.Namespace, policy.Name, evidence),
			sdk.ObjectRefFrom("NetworkPolicy", "networking.k8s.io/v1", &policy),
		))
	}
	return findings, nil
}

// policyIsolation reports a policy that admits no traffic at all in a
// direction it isolates.
func policyIsolation(policy networkingv1.NetworkPolicy) (string, string, string) {
	isolatesIngress, isolatesEgress := false, false
	for _, policyType := range policy.Spec.PolicyTypes {
		isolatesIngress = isolatesIngress || policyType == networkingv1.PolicyTypeIngress
		isolatesEgress = isolatesEgress || policyType == networkingv1.PolicyTypeEgress
	}
	if len(policy.Spec.PolicyTypes) == 0 {
		// The API server defaults an unset list to Ingress, plus Egress when egress rules exist.
		isolatesIngress = true
		isolatesEgress = len(policy.Spec.Egress) > 0
	}
	deniesIngress := isolatesIngress && len(policy.Spec.Ingress) == 0
	deniesEgress := isolatesEgress && len(policy.Spec.Egress) == 0
	switch {
	case deniesIngress && deniesEgress:
		return "network-policy-total-isolation", "A network policy completely isolates selected application pods", "denies all ingress and egress"
	case deniesIngress:
		return "network-policy-denies-all-ingress", "A network policy denies all ingress to selected application pods", "denies all ingress"
	case deniesEgress:
		return "network-policy-denies-all-egress", "A network policy denies all egress from selected application pods", "denies all egress"
	}
	return "", "", ""
}

func policySelectsRequiredWorkload(podSelector metav1.LabelSelector) bool {
	selector, err := metav1.LabelSelectorAsSelector(&podSelector)
	if err != nil {
		return false
	}
	for _, workload := range requiredWorkloadLabels {
		if selector.Matches(labels.Set(workload)) {
			return true
		}
	}
	return false
}

func selectorMatchesLabels(selector, podLabels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if podLabels[key] != value {
			return false
		}
	}
	return true
}

func hasConfigMap(configMaps []corev1.ConfigMap, namespace, name string) bool {
	for _, cm := range configMaps {
		if cm.Namespace == namespace && cm.Name == name {
			return true
		}
	}
	return false
}

func sortedNames(values map[string]struct{}) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func healthFinding(ruleID string, summary string, evidence string, resource sdk.ObjectRef) sdk.Finding {
	return sdk.Finding{
		DetectorID: "health-objective", RuleID: ruleID, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical, Summary: summary, Evidence: evidence,
		PrimaryResource: resource,
		Playbooks:       []string{"health-objective"},
		Fingerprint:     "health-objective/" + ruleID + "/" + resource.Namespace + "/" + resource.Name,
	}
}
